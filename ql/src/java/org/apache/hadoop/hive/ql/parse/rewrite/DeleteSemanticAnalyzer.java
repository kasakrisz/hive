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
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;

public class DeleteSemanticAnalyzer extends RewriteSemanticAnalyzer2 {

  private final RewriterFactory<DeleteBlock> rewriterFactory;

  public DeleteSemanticAnalyzer(QueryState queryState, RewriterFactory<DeleteBlock> rewriterFactory)
      throws SemanticException {
    super(queryState);
    this.rewriterFactory = rewriterFactory;
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode) tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + Context.Operation.DELETE + " but found " + tabName.getName();
    return tabName;
  }

  @Override
  protected void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

    boolean shouldTruncate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_OPTIMIZE_REPLACE_DELETE_WITH_TRUNCATE)
        && children.size() == 1;
    if (shouldTruncate) {
      genTruncatePlan(table, tableName);
      return;
    }

    ASTNode where = null;
    int whereIndex = 1;
    if (children.size() > whereIndex) {
      where = (ASTNode) children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();
    }

    Rewriter<DeleteBlock> rewriter = rewriterFactory.createRewriter(table, getFullTableNameForSQL(tableName));
    ParseUtils.ReparseResult rr = rewriter.rewrite(ctx, new DeleteBlock(table, where));

    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

    updateOutputs(table);
  }

  private void genTruncatePlan(Table table, ASTNode tabNameNode) throws SemanticException {
    String rewrittenQueryStr = "truncate " + getFullTableNameForSQL(tabNameNode);
    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, rewrittenQueryStr);
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    BaseSemanticAnalyzer truncate = SemanticAnalyzerFactory.get(queryState, rewrittenTree);
    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    truncate.analyze(rewrittenTree, rewrittenCtx);

    rootTasks = truncate.getRootTasks();
    outputs = truncate.getOutputs();
    updateOutputs(table);
  }

  public static class DeleteBlock {
    private final Table targetTable;
    private final ASTNode whereTree;


    public DeleteBlock(Table targetTable, ASTNode whereTree) {
      this.targetTable = targetTable;
      this.whereTree = whereTree;
    }

    public Table getTargetTable() {
      return targetTable;
    }

    public ASTNode getWhereTree() {
      return whereTree;
    }
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }
}
