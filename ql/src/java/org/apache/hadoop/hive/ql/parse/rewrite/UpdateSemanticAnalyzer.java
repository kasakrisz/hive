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

package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateSemanticAnalyzer extends RewriteSemanticAnalyzer2 {
  public static final String DELETE_PREFIX = "__d__";
  public static final String SUB_QUERY_ALIAS = "s";

  public UpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode) tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + Context.Operation.UPDATE + " but found " + tabName.getName();
    return tabName;
  }

  @Override
  protected void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

    ASTNode where = null;
    int whereIndex = 2;
    if (children.size() > whereIndex) {
      where = (ASTNode) children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();
    }

//    TOK_UPDATE_TABLE
//            TOK_TABNAME
//               ...
//            TOK_SET_COLUMNS_CLAUSE <- The set list from update should be the second child (index 1)
    assert children.size() >= 2 : "Expected update token to have at least two children";
    ASTNode setClause = (ASTNode) children.get(1);

    Set<String> setRCols = new LinkedHashSet<>();
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(setClause, setRCols, table);
    Map<String, String> colNameToDefaultConstraint = ConstraintsUtils.getColNameToDefaultValueMap(table);

    UpdateBlock updateBlock = new UpdateBlock(table, where, setClause, setCols, colNameToDefaultConstraint);


    boolean splitUpdate = HiveConf.getBoolVar(queryState.getConf(), HiveConf.ConfVars.SPLIT_UPDATE);
    MultiInsertSqlBuilder multiInsertSqlBuilder = getSqlBuilder(splitUpdate ? SUB_QUERY_ALIAS : null, DELETE_PREFIX);
    Rewriter<UpdateBlock> rewriter;
    if (splitUpdate) {
      rewriter = new SplitUpdateRewriter(conf, multiInsertSqlBuilder);
    } else {
      rewriter = new UpdateRewriter(conf, multiInsertSqlBuilder);
    }

    ParseUtils.ReparseResult rr = rewriter.rewrite(ctx, updateBlock);

    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

    updateOutputs(table);

    setUpAccessControlInfoForUpdate(table, setCols);

    // Add the setRCols to the input list
    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
      return;
    }

    for (String colName : setRCols) {
      columnAccessInfo.add(Table.getCompleteName(table.getDbName(), table.getTableName()), colName);
    }
  }

  protected Map<String, ASTNode> collectSetColumnsAndExpressions(
      ASTNode setClause, Set<String> setRCols, Table targetTable) throws SemanticException {
    // An update needs to select all of the columns, as we rewrite the entire row.  Also,
    // we need to figure out which columns we are going to replace.
    assert setClause.getToken().getType() == HiveParser.TOK_SET_COLUMNS_CLAUSE :
        "Expected second child of update token to be set token";

    // Get the children of the set clause, each of which should be a column assignment
    List<? extends Node> assignments = setClause.getChildren();
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, ASTNode> setCols = new LinkedHashMap<String, ASTNode>(assignments.size());
    for (Node a : assignments) {
      ASTNode assignment = (ASTNode)a;
      ASTNode colName = findLHSofAssignment(assignment);
      if (setRCols != null) {
        addSetRCols((ASTNode) assignment.getChildren().get(1), setRCols);
      }
      checkValidSetClauseTarget(colName, targetTable);

      String columnName = normalizeColName(colName.getText());
      // This means that in UPDATE T SET x = _something_
      // _something_ can be whatever is supported in SELECT _something_
      setCols.put(columnName, (ASTNode)assignment.getChildren().get(1));
    }
    return setCols;
  }

  protected ASTNode findLHSofAssignment(ASTNode assignment) {
    assert assignment.getToken().getType() == HiveParser.EQUAL :
        "Expected set assignments to use equals operator but found " + assignment.getName();
    ASTNode tableOrColTok = (ASTNode)assignment.getChildren().get(0);
    assert tableOrColTok.getToken().getType() == HiveParser.TOK_TABLE_OR_COL :
        "Expected left side of assignment to be table or column";
    ASTNode colName = (ASTNode)tableOrColTok.getChildren().get(0);
    assert colName.getToken().getType() == HiveParser.Identifier :
        "Expected column name";
    return colName;
  }
  // This method finds any columns on the right side of a set statement (thus rcols) and puts them
  // in a set so we can add them to the list of input cols to check.

  private void addSetRCols(ASTNode node, Set<String> setRCols) {

    // See if this node is a TOK_TABLE_OR_COL.  If so, find the value and put it in the list.  If
    // not, recurse on any children
    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      ASTNode colName = (ASTNode)node.getChildren().get(0);
      if (colName.getToken().getType() == HiveParser.TOK_DEFAULT_VALUE) {
        return;
      }

      assert colName.getToken().getType() == HiveParser.Identifier :
          "Expected column name";
      setRCols.add(normalizeColName(colName.getText()));
    } else if (node.getChildren() != null) {
      for (Node n : node.getChildren()) {
        addSetRCols((ASTNode)n, setRCols);
      }
    }
  }

  /**
   * Assert that we are not asked to update a bucketing column or partition column.
   * @param colName it's the A in "SET A = B"
   */
  protected void checkValidSetClauseTarget(ASTNode colName, Table targetTable) throws SemanticException {
    String columnName = normalizeColName(colName.getText());

    // Make sure this isn't one of the partitioning columns, that's not supported.
    for (FieldSchema fschema : targetTable.getPartCols()) {
      if (fschema.getName().equalsIgnoreCase(columnName)) {
        throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_PART_VALUE.getMsg());
      }
    }
    //updating bucket column should move row from one file to another - not supported
    if (targetTable.getBucketCols() != null && targetTable.getBucketCols().contains(columnName)) {
      throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_BUCKET_VALUE, columnName);
    }
    boolean foundColumnInTargetTable = false;
    for (FieldSchema col : targetTable.getCols()) {
      if (columnName.equalsIgnoreCase(col.getName())) {
        foundColumnInTargetTable = true;
        break;
      }
    }
    if (!foundColumnInTargetTable) {
      throw new SemanticException(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE, colName.getText(),
          targetTable.getFullyQualifiedName());
    }
  }

  /**
   * Column names are stored in metastore in lower case, regardless of the CREATE TABLE statement.
   * Unfortunately there is no single place that normalizes the input query.
   * @param colName not null
   */
  private static String normalizeColName(String colName) {
    return colName.toLowerCase();
  }

  public static class UpdateBlock {
    private final Table targetTable;
    private final ASTNode whereTree;
    private final ASTNode setClauseTree;
    private final Map<String, ASTNode> setCols;
    private final Map<String, String> colNameToDefaultConstraint;


    public UpdateBlock(Table targetTable, ASTNode whereTree, ASTNode setClauseTree, Map<String, ASTNode> setCols, Map<String, String> colNameToDefaultConstraint) {
      this.targetTable = targetTable;
      this.whereTree = whereTree;
      this.setClauseTree = setClauseTree;
      this.setCols = setCols;
      this.colNameToDefaultConstraint = colNameToDefaultConstraint;
    }

    public Table getTargetTable() {
      return targetTable;
    }

    public ASTNode getWhereTree() {
      return whereTree;
    }

    public ASTNode getSetClauseTree() {
      return setClauseTree;
    }

    public Map<String, ASTNode> getSetCols() {
      return setCols;
    }

    public Map<String, String> getColNameToDefaultConstraint() {
      return colNameToDefaultConstraint;
    }
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }
}
