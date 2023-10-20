package org.apache.hadoop.hive.ql.parse.rewrite;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RewriteSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.UnparseTranslator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils.getColNameToDefaultValueMap;
import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.hadoop.hive.ql.parse.rewrite.ASTRewriteUtils.addPartitionColsAsValues;
import static org.apache.hadoop.hive.ql.parse.rewrite.ASTRewriteUtils.collectSetColumnsAndExpressions;

public class MergeRewriter implements Rewriter<MergeSemanticAnalyzer.MergeBlock> {

  private final HiveConf conf;
  private final MultiInsertSqlBuilder sqlBuilder;
  private final TokenRewriteStream tokenRewriteStream;
  private final IdentifierQuoter identifierQuoter;

  public MergeRewriter(HiveConf conf, MultiInsertSqlBuilder sqlBuilder, TokenRewriteStream tokenRewriteStream) {
    this.conf = conf;
    this.sqlBuilder = sqlBuilder;
    this.tokenRewriteStream = tokenRewriteStream;
    this.identifierQuoter = new IdentifierQuoter(tokenRewriteStream);
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, MergeSemanticAnalyzer.MergeBlock mergeBlock)
      throws SemanticException {
    String sourceText = getMatchedText(mergeBlock.getSourceTree());
    String onClauseAsText = getMatchedText(mergeBlock.getOnClause());

    sqlBuilder.append("(SELECT ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.MERGE);

    sqlBuilder.removeLastChar();
    sqlBuilder.appendCols(sqlBuilder.getTargetTable().getPartCols());
    sqlBuilder.appendColsOfTargetTable();
    sqlBuilder.append(" FROM ").appendTargetTableName().append(") ");
    sqlBuilder.append(mergeBlock.getSubQueryAlias());
    sqlBuilder.append('\n');

    sqlBuilder.indent().append(chooseJoinType(mergeBlock.getWhenClauses())).append("\n");
    if (mergeBlock.getSourceTree().getType() == HiveParser.TOK_SUBQUERY) {
      //this includes the mandatory alias
      sqlBuilder.indent().append(sourceText);
    } else {
      sqlBuilder.indent().append(mergeBlock.getSourceFullTableName());
    }
    sqlBuilder.append('\n');
    sqlBuilder.indent().append("ON ").append(onClauseAsText).append('\n');

    /**
     * We allow at most 2 WHEN MATCHED clause, in which case 1 must be Update the other Delete
     * If we have both update and delete, the 1st one (in SQL code) must have "AND <extra predicate>"
     * so that the 2nd can ensure not to process the same rows.
     * Update and Delete may be in any order.  (Insert is always last)
     */
    String extraPredicate = null;
    int numInsertClauses = 0;
    int numWhenMatchedUpdateClauses = 0;
    int numWhenMatchedDeleteClauses = 0;
    boolean hintProcessed = false;
    for (ASTNode whenClause : mergeBlock.getWhenClauses()) {
      switch (getWhenClauseOperation(whenClause).getType()) {
        case HiveParser.TOK_INSERT:
          numInsertClauses++;
          handleInsert(whenClause, mergeBlock.getOnClause(), sqlBuilder.getTargetTable(), mergeBlock.getTargetName(),
              onClauseAsText, hintProcessed ? null : mergeBlock.getHintStr());
          hintProcessed = true;
          break;
        case HiveParser.TOK_UPDATE:
          numWhenMatchedUpdateClauses++;
          String s = handleUpdate(whenClause, onClauseAsText, mergeBlock.getTargetTable(), mergeBlock.getTargetName(),
              extraPredicate, hintProcessed ? null : mergeBlock.getHintStr());

          hintProcessed = true;
          if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
            extraPredicate = s; //i.e. it's the 1st WHEN MATCHED
          }
          break;
        case HiveParser.TOK_DELETE:
          numWhenMatchedDeleteClauses++;
          String s1 = handleDelete(whenClause, rewrittenQueryStr,
              onClauseAsText, extraPredicate, hintProcessed ? null : hintStr, columnAppender);
          hintProcessed = true;
          if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
            extraPredicate = s1; //i.e. it's the 1st WHEN MATCHED
          }
          break;
        default:
          throw new IllegalStateException("Unexpected WHEN clause type: " + whenClause.getType() +
              addParseInfo(whenClause));
      }
      if (numWhenMatchedDeleteClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_DELETE, ctx.getCmd());
      }
      if (numWhenMatchedUpdateClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_UPDATE, ctx.getCmd());
      }
      assert numInsertClauses < 2: "too many Insert clauses";
    }
    if (numWhenMatchedDeleteClauses + numWhenMatchedUpdateClauses == 2 && extraPredicate == null) {
      throw new SemanticException(ErrorMsg.MERGE_PREDIACTE_REQUIRED, ctx.getCmd());
    }

    boolean validating = handleCardinalityViolation(rewrittenQueryStr, targetNameNode, onClauseAsText, targetTable,
        numWhenMatchedDeleteClauses == 0 && numWhenMatchedUpdateClauses == 0, columnAppender);
    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, rewrittenQueryStr);
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;
    rewrittenCtx.setOperation(Context.Operation.MERGE);

    //set dest name mapping on new context; 1st child is TOK_FROM
    int insClauseIdx = 1;
    for (int whenClauseIdx = 0;
         insClauseIdx < rewrittenTree.getChildCount() - (validating ? 1 : 0/*skip cardinality violation clause*/);
         whenClauseIdx++) {
      //we've added Insert clauses in order or WHEN items in whenClauses
      switch (getWhenClauseOperation(whenClauses.get(whenClauseIdx)).getType()) {
        case HiveParser.TOK_INSERT:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
          ++insClauseIdx;
          break;
        case HiveParser.TOK_UPDATE:
          insClauseIdx += addDestNamePrefixOfUpdate(insClauseIdx, rewrittenCtx);
          break;
        case HiveParser.TOK_DELETE:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.DELETE);
          ++insClauseIdx;
          break;
        default:
          assert false;
      }
    }
    if (validating) {
      //here means the last branch of the multi-insert is Cardinality Validation
      rewrittenCtx.addDestNamePrefix(rewrittenTree.getChildCount() - 1, Context.DestClausePrefix.INSERT);
    }

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);
    updateOutputs(targetTable);

    return null;
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

  protected ASTNode getWhenClauseOperation(ASTNode whenClause) {
    if (!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    return (ASTNode) whenClause.getChild(0);
  }

  /**
   * Generates the Insert leg of the multi-insert SQL to represent WHEN NOT MATCHED THEN INSERT clause.
   * @param targetTableNameInSourceQuery - simple name/alias
   * @throws SemanticException
   */
  private void handleInsert(ASTNode whenNotMatchedClause, ASTNode onClause, Table targetTable,
                            String targetTableNameInSourceQuery, String onClauseAsString,
                            String hintStr) throws SemanticException {
    ASTNode whenClauseOperation = getWhenClauseOperation(whenNotMatchedClause);
    assert whenNotMatchedClause.getType() == HiveParser.TOK_NOT_MATCHED;
    assert whenClauseOperation.getType() == HiveParser.TOK_INSERT;

    // identify the node that contains the values to insert and the optional column list node
    List<Node> children = whenClauseOperation.getChildren();
    ASTNode valuesNode =
        (ASTNode)children.stream().filter(n -> ((ASTNode)n).getType() == HiveParser.TOK_FUNCTION).findFirst().get();
    ASTNode columnListNode =
        (ASTNode)children.stream().filter(n -> ((ASTNode)n).getType() == HiveParser.TOK_TABCOLNAME).findFirst()
            .orElse(null);

    // if column list is specified, then it has to have the same number of elements as the values
    // valuesNode has a child for struct, the rest are the columns
    if (columnListNode != null && columnListNode.getChildCount() != (valuesNode.getChildCount() - 1)) {
      throw new SemanticException(String.format("Column schema must have the same length as values (%d vs %d)",
          columnListNode.getChildCount(), valuesNode.getChildCount() - 1));
    }

    sqlBuilder.append("INSERT INTO ").appendTargetTableName();
    if (columnListNode != null) {
      sqlBuilder.append(' ').append(getMatchedText(columnListNode));
    }

    sqlBuilder.append("    -- insert clause\n  SELECT ");
    if (hintStr != null) {
      sqlBuilder.append(hintStr);
    }

    OnClauseAnalyzer oca = new OnClauseAnalyzer(
        onClause, targetTable, targetTableNameInSourceQuery, conf, onClauseAsString);
    oca.analyze();

    UnparseTranslator defaultValuesTranslator = new UnparseTranslator(conf);
    defaultValuesTranslator.enable();
    List<String> targetSchema = processTableColumnNames(columnListNode, targetTable.getFullyQualifiedName());
    collectDefaultValues(valuesNode, targetTable, targetSchema, defaultValuesTranslator);
    defaultValuesTranslator.applyTranslations(tokenRewriteStream);
    String valuesClause = getMatchedText(valuesNode);
    valuesClause = valuesClause.substring(1, valuesClause.length() - 1); //strip '(' and ')'
    sqlBuilder.append(valuesClause).append("\n   WHERE ").append(oca.getPredicate());

    String extraPredicate = getWhenClausePredicate(whenNotMatchedClause);
    if (extraPredicate != null) {
      //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
      sqlBuilder.append(" AND ")
          .append(getMatchedText(((ASTNode)whenNotMatchedClause.getChild(1))));
    }
    sqlBuilder.append('\n');
  }

  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param deleteExtraPredicate - see notes at caller
   */
  private String handleUpdate(ASTNode whenMatchedUpdateClause, String onClauseAsString, Table targetTable,
                              String targetName, String deleteExtraPredicate, String hintStr)
      throws SemanticException {
    assert whenMatchedUpdateClause.getType() == HiveParser.TOK_MATCHED;
    assert getWhenClauseOperation(whenMatchedUpdateClause).getType() == HiveParser.TOK_UPDATE;
    List<String> values = new ArrayList<>(targetTable.getCols().size());

    ASTNode setClause = (ASTNode)getWhenClauseOperation(whenMatchedUpdateClause).getChild(0);
    //columns being updated -> update expressions; "setRCols" (last param) is null because we use actual expressions
    //before re-parsing, i.e. they are known to SemanticAnalyzer logic
    Map<String, ASTNode> setColsExprs = collectSetColumnsAndExpressions(setClause, null, targetTable);
    //if target table has cols c1,c2,c3 and p1 partition col and we had "SET c2 = 5, c1 = current_date()" we want to end
    //up with
    //insert into target (p1) select current_date(), 5, c3, p1 where ....
    //since we take the RHS of set exactly as it was in Input, we don't need to deal with quoting/escaping column/table
    //names
    List<FieldSchema> nonPartCols = targetTable.getCols();
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(targetTable);
    for (FieldSchema fs : nonPartCols) {
      String name = fs.getName();
      if (setColsExprs.containsKey(name)) {
        ASTNode setColExpr = setColsExprs.get(name);
        if (setColExpr.getType() == HiveParser.TOK_TABLE_OR_COL &&
            setColExpr.getChildCount() == 1 && setColExpr.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          UnparseTranslator defaultValueTranslator = new UnparseTranslator(conf);
          defaultValueTranslator.enable();
          defaultValueTranslator.addDefaultValueTranslation(
              setColsExprs.get(name), colNameToDefaultConstraint.get(name));
          defaultValueTranslator.applyTranslations(tokenRewriteStream);
        }

        String rhsExp = getMatchedText(setColsExprs.get(name));
        //"set a=5, b=8" - rhsExp picks up the next char (e.g. ',') from the token stream
        switch (rhsExp.charAt(rhsExp.length() - 1)) {
          case ',':
          case '\n':
            rhsExp = rhsExp.substring(0, rhsExp.length() - 1);
            break;
          default:
            //do nothing
        }

        values.add(rhsExp);
      } else {
        values.add(targetName + "." + HiveUtils.unparseIdentifier(name, this.conf));
      }
    }
    addPartitionColsAsValues(targetTable.getPartCols(), targetName, values);

    String extraPredicate = handleUpdate(whenMatchedUpdateClause, onClauseAsString,
        deleteExtraPredicate, hintStr, targetName, values);

    setUpAccessControlInfoForUpdate(targetTable, setColsExprs);
    return extraPredicate;
  }

  protected String handleUpdate(ASTNode whenMatchedUpdateClause,
                                String onClauseAsString, String deleteExtraPredicate, String hintStr,
                                String targetName, List<String> values) {
    values.add(0, targetName + ".ROW__ID");

    sqlBuilder.append("    -- update clause").append("\n");
    sqlBuilder.appendInsertBranch(hintStr, values);

    String extraPredicate = addWhereClauseOfUpdate(onClauseAsString, whenMatchedUpdateClause, deleteExtraPredicate);

    sqlBuilder.appendSortBy(Collections.singletonList(targetName + ".ROW__ID "));
    sqlBuilder.append("\n");

    return extraPredicate;
  }

  protected List<String> processTableColumnNames(ASTNode tabColName, String tableName) throws SemanticException {
    if (tabColName == null) {
      return Collections.emptyList();
    }
    List<String> targetColNames = new ArrayList<>(tabColName.getChildren().size());
    for(Node col : tabColName.getChildren()) {
      assert ((ASTNode)col).getType() == HiveParser.Identifier :
          "expected token " + HiveParser.Identifier + " found " + ((ASTNode)col).getType();
      targetColNames.add(((ASTNode)col).getText().toLowerCase());
    }
    Set<String> targetColumns = new HashSet<>(targetColNames);
    if(targetColNames.size() != targetColumns.size()) {
      throw new SemanticException(generateErrorMessage(tabColName,
          "Duplicate column name detected in " + tableName + " table schema specification"));
    }
    return targetColNames;
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
  private String getMatchedText(ASTNode n) {
    identifierQuoter.visit(n);
    return tokenRewriteStream.toString(n.getTokenStartIndex(),
        n.getTokenStopIndex() + 1).trim();
  }

  private void collectDefaultValues(
      ASTNode valueClause, Table targetTable, List<String> targetSchema, UnparseTranslator unparseTranslator)
      throws SemanticException {
    List<String> defaultConstraints = getDefaultConstraints(targetTable, targetSchema);
    for (int j = 0; j < defaultConstraints.size(); j++) {
      unparseTranslator.addDefaultValueTranslation((ASTNode) valueClause.getChild(j + 1), defaultConstraints.get(j));
    }
  }

  /**
   * Returns the <boolean predicate> as in WHEN MATCHED AND <boolean predicate> THEN...
   * @return may be null
   */
  private String getWhenClausePredicate(ASTNode whenClause) {
    if (!(whenClause.getType() == HiveParser.TOK_MATCHED || whenClause.getType() == HiveParser.TOK_NOT_MATCHED)) {
      throw raiseWrongType("Expected TOK_MATCHED|TOK_NOT_MATCHED", whenClause);
    }
    if (whenClause.getChildCount() == 2) {
      return getMatchedText((ASTNode)whenClause.getChild(1));
    }
    return null;
  }

  protected String addWhereClauseOfUpdate(String onClauseAsString,
                                          ASTNode whenMatchedUpdateClause, String deleteExtraPredicate) {
    sqlBuilder.indent().append("WHERE ").append(onClauseAsString);
    String extraPredicate = getWhenClausePredicate(whenMatchedUpdateClause);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      sqlBuilder.append(" AND ").append(extraPredicate);
    }
    if (deleteExtraPredicate != null) {
      sqlBuilder.append(" AND NOT(").append(deleteExtraPredicate).append(")");
    }

    return extraPredicate;
  }
}
