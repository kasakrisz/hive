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
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;
import java.util.Map;

public class UpdateSemanticAnalyzer extends RewriteSemanticAnalyzer2 {

  private final RewriterFactory<UpdateBlock> rewriterFactory;

  public UpdateSemanticAnalyzer(QueryState queryState, RewriterFactory<UpdateBlock> rewriterFactory)
      throws SemanticException {
    super(queryState);
    this.rewriterFactory = rewriterFactory;
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

    SetClauseHandler.Result setCols = new SetClauseHandler().handle(setClause, table);
    Map<String, String> colNameToDefaultConstraint = ConstraintsUtils.getColNameToDefaultValueMap(table);

    UpdateBlock updateBlock = new UpdateBlock(
        table, where, setClause, setCols.getSetExpressions(), colNameToDefaultConstraint);

    Rewriter<UpdateBlock> rewriter = rewriterFactory.createRewriter(table, getFullTableNameForSQL(tableName));

    ParseUtils.ReparseResult rr = rewriter.rewrite(ctx, updateBlock);

    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

    updateOutputs(table);

    setUpAccessControlInfoForUpdate(table, setCols.getSetExpressions());

    // Add the setRCols to the input list
    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
      return;
    }

    for (String colName : setCols.getSetRCols()) {
      columnAccessInfo.add(Table.getCompleteName(table.getDbName(), table.getTableName()), colName);
    }
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
