/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A subclass of the {@link SemanticAnalyzer} that just handles
 * merge statements. It works by rewriting the updates and deletes into insert statements (since
 * they are actually inserts) and then doing some patch up to make them work as merges instead.
 */
public abstract class SplitRewriteSemanticAnalyzer extends RewriteSemanticAnalyzer {
  SplitRewriteSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected abstract void analyze(ASTNode tree) throws SemanticException;

  private static final String INDENT = "  ";

  private IdentifierQuoter quotedIdentifierHelper;

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it.
   * Since HiveLexer.g is written such that it strips away any ` (back ticks) around
   * quoted identifiers we need to add those back to generated SQL.
   * Additionally, the parser only produces tokens of type Identifier and never
   * QuotedIdentifier (HIVE-6013).  So here we just quote all identifiers.
   * (') around String literals are retained w/o issues
   */
  private static class IdentifierQuoter {
    private final TokenRewriteStream trs;
    private final IdentityHashMap<ASTNode, ASTNode> visitedNodes = new IdentityHashMap<>();

    IdentifierQuoter(TokenRewriteStream trs) {
      this.trs = trs;
      if (trs == null) {
        throw new IllegalArgumentException("Must have a TokenRewriteStream");
      }
    }

    private void visit(ASTNode n) {
      if (n.getType() == HiveParser.Identifier) {
        if (visitedNodes.containsKey(n)) {
          /**
           * Since we are modifying the stream, it's not idempotent.  Ideally, the caller would take
           * care to only quote Identifiers in each subtree once, but this makes it safe
           */
          return;
        }
        visitedNodes.put(n, n);
        trs.insertBefore(n.getToken(), "`");
        trs.insertAfter(n.getToken(), "`");
      }
      if (n.getChildCount() <= 0) {
        return;
      }
      for (Node c : n.getChildren()) {
        visit((ASTNode)c);
      }
    }
  }

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it without
   * needing to understand what it is (except for QuotedIdentifiers).
   */
  protected String getMatchedText(ASTNode n) {
    quotedIdentifierHelper.visit(n);
    return ctx.getTokenRewriteStream().toString(n.getTokenStartIndex(),
      n.getTokenStopIndex() + 1).trim();
  }

  private StringBuilder rewrittenQueryStr;

  protected void initRewrittenQueryStr() {
    rewrittenQueryStr = new StringBuilder("FROM\n");
  }

  private String targetTableName;

  protected void appendTarget(ASTNode target, String targetName) throws SemanticException {
    targetTableName = getFullTableNameForSQL(target);
    rewrittenQueryStr.append(INDENT).append(targetTableName);
    if (isAliased(target)) {
      rewrittenQueryStr.append(" ").append(targetName);
    }
    rewrittenQueryStr.append('\n');
  }

  protected void appendSource(List<ASTNode> whenClauses, ASTNode source, String sourceName, String onClauseAsText)
          throws SemanticException {
    rewrittenQueryStr.append(INDENT).append(chooseJoinType(whenClauses)).append("\n");
    if (source.getType() == HiveParser.TOK_SUBQUERY) {
      //this includes the mandatory alias
      rewrittenQueryStr.append(INDENT).append(getMatchedText(source));
    } else {
      rewrittenQueryStr.append(INDENT).append(getFullTableNameForSQL(source));
      if (isAliased(source)) {
        rewrittenQueryStr.append(" ").append(sourceName);
      }
    }
    rewrittenQueryStr.append('\n');
    rewrittenQueryStr.append(INDENT).append("ON ").append(onClauseAsText).append('\n');
  }

  protected void appendInsertBranch(List<String> columnList, List<String> values, Table targetTable)
          throws SemanticException {
    rewrittenQueryStr.append("INSERT INTO ").append(targetTableName);
    if (columnList != null) {
      if (columnList.size() != values.size()) {
        throw new SemanticException(String.format("Column schema must have the same length as values (%d vs %d)",
                columnList.size(), values.size()));
      }

      rewrittenQueryStr.append("(");
      rewrittenQueryStr.append(StringUtils.join(columnList, ","));
      rewrittenQueryStr.append(")");
    }

    rewrittenQueryStr.append("    -- insert clause\n  SELECT ");
    // TODO: hint
//    if (hintStr != null) {
//      rewrittenQueryStr.append(hintStr);
//    }

    rewrittenQueryStr.append(StringUtils.join(values, ","));

//    String valuesClause = getMatchedText(valuesNode);
//    valuesClause = valuesClause.substring(1, valuesClause.length() - 1); //strip '(' and ')'
//    valuesClause = replaceDefaultKeywordForMerge(valuesClause, targetTable, columnListNode);
//    rewrittenQueryStr.append(valuesClause);
    rewrittenQueryStr.append('\n');
  }

  protected void appendWhereClause(String predicate) {
    rewrittenQueryStr.append(INDENT).append("WHERE ").append(predicate).append("\n");
  }

  protected void appendSortBy(List<String> keys) {
    rewrittenQueryStr.append("\n SORT BY ");
    rewrittenQueryStr.append(StringUtils.join(keys, ","));
  }

  /**
   * Here we take a Merge statement AST and generate a semantically equivalent multi-insert
   * statement to execute.  Each Insert leg represents a single WHEN clause.  As much as possible,
   * the new SQL statement is made to look like the input SQL statement so that it's easier to map
   * Query Compiler errors from generated SQL to original one this way.
   * The generated SQL is a complete representation of the original input for the same reason.
   * In many places SemanticAnalyzer throws exceptions that contain (line, position) coordinates.
   * If generated SQL doesn't have everything and is patched up later, these coordinates point to
   * the wrong place.
   *
   * @throws SemanticException
   */

  protected ReparseResult parseRewrittenQuery(String originalQuery) throws SemanticException {
    return super.parseRewrittenQuery(rewrittenQueryStr, originalQuery);
  }

  /**
   * If there is no WHEN NOT MATCHED THEN INSERT, we don't outer join.
   */
  private String chooseJoinType(List<ASTNode> whenClauses) {
    for (ASTNode whenClause : whenClauses) {
      if (getWhenClauseOperation(whenClause).getType() == HiveParser.TOK_INSERT) {
        return "RIGHT OUTER JOIN";
      }
    }
    return "INNER JOIN";
  }

  /**
   * Per SQL Spec ISO/IEC 9075-2:2011(E) Section 14.2 under "General Rules" Item 6/Subitem a/Subitem 2/Subitem B,
   * an error should be raised if > 1 row of "source" matches the same row in "target".
   * This should not affect the runtime of the query as it's running in parallel with other
   * branches of the multi-insert.  It won't actually write any data to merge_tmp_table since the
   * cardinality_violation() UDF throws an error whenever it's called killing the query
   * @return true if another Insert clause was added
   */
  protected boolean handleCardinalityViolation(ASTNode target,
      String onClauseAsString, Table targetTable, boolean onlyHaveWhenNotMatchedClause)
              throws SemanticException {
    if (!conf.getBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK)) {
      LOG.info("Merge statement cardinality violation check is disabled: " +
          HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK.varname);
      return false;
    }
    if (onlyHaveWhenNotMatchedClause) {
      //if no update or delete in Merge, there is no need to to do cardinality check
      return false;
    }
    //this is a tmp table and thus Session scoped and acid requires SQL statement to be serial in a
    // given session, i.e. the name can be fixed across all invocations
    String tableName = "merge_tmp_table";
    rewrittenQueryStr.append("INSERT INTO ").append(tableName)
      .append("\n  SELECT cardinality_violation(")
      .append(getSimpleTableName(target)).append(".ROW__ID");
    addPartitionColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);

    rewrittenQueryStr.append(")\n WHERE ").append(onClauseAsString)
      .append(" GROUP BY ").append(getSimpleTableName(target)).append(".ROW__ID");

    addPartitionColsToSelect(targetTable.getPartCols(), rewrittenQueryStr, target);

    rewrittenQueryStr.append(" HAVING count(*) > 1");
    //say table T has partition p, we are generating
    //select cardinality_violation(ROW_ID, p) WHERE ... GROUP BY ROW__ID, p
    //the Group By args are passed to cardinality_violation to add the violating value to the error msg
    try {
      if (null == db.getTable(tableName, false)) {
        StorageFormat format = new StorageFormat(conf);
        format.processStorageFormat("TextFile");
        Table table = db.newTable(tableName);
        table.setSerializationLib(format.getSerde());
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("val", "int", null));
        table.setFields(fields);
        table.setDataLocation(Warehouse.getDnsPath(new Path(SessionState.get().getTempTableSpace(),
            tableName), conf));
        table.getTTable().setTemporary(true);
        table.setStoredAsSubDirectories(false);
        table.setInputFormatClass(format.getInputFormat());
        table.setOutputFormatClass(format.getOutputFormat());
        db.createTable(table, true);
      }
    } catch(HiveException|MetaException e) {
      throw new SemanticException(e.getMessage(), e);
    }
    return true;
  }

  private static String addParseInfo(ASTNode n) {
    return " at " + ASTErrorUtils.renderPosition(n);
  }

  private boolean isAliased(ASTNode n) {
    switch (n.getType()) {
    case HiveParser.TOK_TABREF:
      return findTabRefIdxs(n)[0] != 0;
    case HiveParser.TOK_TABNAME:
      return false;
    case HiveParser.TOK_SUBQUERY:
      assert n.getChildCount() > 1 : "Expected Derived Table to be aliased";
      return true;
    default:
      throw raiseWrongType("TOK_TABREF|TOK_TABNAME", n);
    }
  }

  /**
   * Collect WHEN clauses from Merge statement AST.
   */
  private List<ASTNode> findWhenClauses(ASTNode tree, int start) throws SemanticException {
    assert tree.getType() == HiveParser.TOK_MERGE;
    List<ASTNode> whenClauses = new ArrayList<>();
    for (int idx = start; idx < tree.getChildCount(); idx++) {
      ASTNode whenClause = (ASTNode)tree.getChild(idx);
      assert whenClause.getType() == HiveParser.TOK_MATCHED ||
        whenClause.getType() == HiveParser.TOK_NOT_MATCHED :
        "Unexpected node type found: " + whenClause.getType() + addParseInfo(whenClause);
      whenClauses.add(whenClause);
    }
    if (whenClauses.size() <= 0) {
      //Futureproofing: the parser will actually not allow this
      throw new SemanticException("Must have at least 1 WHEN clause in MERGE statement");
    }
    return whenClauses;
  }

  private ASTNode getWhenClauseOperation(ASTNode whenClause) {
    if (!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw  raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    return (ASTNode) whenClause.getChild(0);
  }

  private String replaceDefaultKeywordForMerge(String valueClause, Table table, ASTNode columnListNode)
      throws SemanticException {
    if (!valueClause.toLowerCase().contains("`default`")) {
      return valueClause;
    }

    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(table);
    String[] values = valueClause.trim().split(",");
    String[] replacedValues = new String[values.length];

    // the list of the column names may be set in the query
    String[] columnNames = columnListNode == null ?
      table.getAllCols().stream().map(f -> f.getName()).toArray(size -> new String[size]) :
      columnListNode.getChildren().stream().map(n -> ((ASTNode)n).toString()).toArray(size -> new String[size]);

    for (int i = 0; i < values.length; i++) {
      if (values[i].trim().toLowerCase().equals("`default`")) {
        replacedValues[i] = MapUtils.getString(colNameToDefaultConstraint, columnNames[i], "null");
      } else {
        replacedValues[i] = values[i];
      }
    }
    return StringUtils.join(replacedValues, ',');
  }

  /**
   * Suppose the input Merge statement has ON target.a = source.b and c = d.  Assume, that 'c' is from
   * target table and 'd' is from source expression.  In order to properly
   * generate the Insert for WHEN NOT MATCHED THEN INSERT, we need to make sure that the Where
   * clause of this Insert contains "target.a is null and target.c is null"  This ensures that this
   * Insert leg does not receive any rows that are processed by Insert corresponding to
   * WHEN MATCHED THEN ... clauses.  (Implicit in this is a mini resolver that figures out if an
   * unqualified column is part of the target table.  We can get away with this simple logic because
   * we know that target is always a table (as opposed to some derived table).
   * The job of this class is to generate this predicate.
   *
   * Note that is this predicate cannot simply be NOT(on-clause-expr).  IF on-clause-expr evaluates
   * to Unknown, it will be treated as False in the WHEN MATCHED Inserts but NOT(Unknown) = Unknown,
   * and so it will be False for WHEN NOT MATCHED Insert...
   */
  private static final class OnClauseAnalyzer {
    private final ASTNode onClause;
    private final Map<String, List<String>> table2column = new HashMap<>();
    private final List<String> unresolvedColumns = new ArrayList<>();
    private final List<FieldSchema> allTargetTableColumns = new ArrayList<>();
    private final Set<String> tableNamesFound = new HashSet<>();
    private final String targetTableNameInSourceQuery;
    private final HiveConf conf;
    private final String onClauseAsString;

    /**
     * @param targetTableNameInSourceQuery alias or simple name
     */
    OnClauseAnalyzer(ASTNode onClause, Table targetTable, String targetTableNameInSourceQuery,
                     HiveConf conf, String onClauseAsString) {
      this.onClause = onClause;
      allTargetTableColumns.addAll(targetTable.getCols());
      allTargetTableColumns.addAll(targetTable.getPartCols());
      this.targetTableNameInSourceQuery = unescapeIdentifier(targetTableNameInSourceQuery);
      this.conf = conf;
      this.onClauseAsString = onClauseAsString;
    }

    /**
     * Finds all columns and groups by table ref (if there is one).
     */
    private void visit(ASTNode n) {
      if (n.getType() == HiveParser.TOK_TABLE_OR_COL) {
        ASTNode parent = (ASTNode) n.getParent();
        if (parent != null && parent.getType() == HiveParser.DOT) {
          //the ref must be a table, so look for column name as right child of DOT
          if (parent.getParent() != null && parent.getParent().getType() == HiveParser.DOT) {
            //I don't think this can happen... but just in case
            throw new IllegalArgumentException("Found unexpected db.table.col reference in " + onClauseAsString);
          }
          addColumn2Table(n.getChild(0).getText(), parent.getChild(1).getText());
        } else {
          //must be just a column name
          unresolvedColumns.add(n.getChild(0).getText());
        }
      }
      if (n.getChildCount() == 0) {
        return;
      }
      for (Node child : n.getChildren()) {
        visit((ASTNode)child);
      }
    }

    private void analyze() {
      visit(onClause);
      if (tableNamesFound.size() > 2) {
        throw new IllegalArgumentException("Found > 2 table refs in ON clause.  Found " +
          tableNamesFound + " in " + onClauseAsString);
      }
      handleUnresolvedColumns();
      if (tableNamesFound.size() > 2) {
        throw new IllegalArgumentException("Found > 2 table refs in ON clause (incl unresolved).  " +
          "Found " + tableNamesFound + " in " + onClauseAsString);
      }
    }

    /**
     * Find those that belong to target table.
     */
    private void handleUnresolvedColumns() {
      if (unresolvedColumns.isEmpty()) {
        return;
      }
      for (String c : unresolvedColumns) {
        for (FieldSchema fs : allTargetTableColumns) {
          if (c.equalsIgnoreCase(fs.getName())) {
            //c belongs to target table; strictly speaking there maybe an ambiguous ref but
            //this will be caught later when multi-insert is parsed
            addColumn2Table(targetTableNameInSourceQuery.toLowerCase(), c);
            break;
          }
        }
      }
    }

    private void addColumn2Table(String tableName, String columnName) {
      tableName = tableName.toLowerCase(); //normalize name for mapping
      tableNamesFound.add(tableName);
      List<String> cols = table2column.get(tableName);
      if (cols == null) {
        cols = new ArrayList<>();
        table2column.put(tableName, cols);
      }
      //we want to preserve 'columnName' as it was in original input query so that rewrite
      //looks as much as possible like original query
      cols.add(columnName);
    }

    /**
     * Now generate the predicate for Where clause.
     */
    private String getPredicate() {
      //normilize table name for mapping
      List<String> targetCols = table2column.get(targetTableNameInSourceQuery.toLowerCase());
      if (targetCols == null) {
        /*e.g. ON source.t=1
        * this is not strictly speaking invalid but it does ensure that all columns from target
        * table are all NULL for every row.  This would make any WHEN MATCHED clause invalid since
        * we don't have a ROW__ID.  The WHEN NOT MATCHED could be meaningful but it's just data from
        * source satisfying source.t=1...  not worth the effort to support this*/
        throw new IllegalArgumentException(ErrorMsg.INVALID_TABLE_IN_ON_CLAUSE_OF_MERGE
          .format(targetTableNameInSourceQuery, onClauseAsString));
      }
      StringBuilder sb = new StringBuilder();
      for (String col : targetCols) {
        if (sb.length() > 0) {
          sb.append(" AND ");
        }
        //but preserve table name in SQL
        sb.append(HiveUtils.unparseIdentifier(targetTableNameInSourceQuery, conf))
          .append(".")
          .append(HiveUtils.unparseIdentifier(col, conf))
          .append(" IS NULL");
      }
      return sb.toString();
    }
  }
}
