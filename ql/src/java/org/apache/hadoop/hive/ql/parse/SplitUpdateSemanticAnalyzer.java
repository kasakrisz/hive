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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A subclass of the {@link SemanticAnalyzer} that just handles
 * update and delete statements. It works by rewriting the updates and deletes into insert
 * statements (since they are actually inserts) and then doing some patch up to make them work as
 * updates and deletes instead.
 */
public class SplitUpdateSemanticAnalyzer extends RewriteSemanticAnalyzer {

  private Context.Operation operation = Context.Operation.OTHER;

  SplitUpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  protected void analyze(ASTNode tree) throws SemanticException {
    if (tree.getToken().getType() != HiveParser.TOK_UPDATE_TABLE) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in SplitUpdateSemanticAnalyzer");
    }

    analyzeUpdate(tree);
  }

  private void analyzeUpdate(ASTNode tree) throws SemanticException {
    operation = Context.Operation.UPDATE;
    reparseAndSuperAnalyze(tree);
  }

  /**
   * This supports update statements
   */
  private void reparseAndSuperAnalyze(ASTNode tree) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

    // The first child should be the table we are updating
    ASTNode tabName = (ASTNode) children.get(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
            "Expected tablename as first child of " + operation + " but found " + tabName.getName();
    Table mTable = getTargetTable(tabName);
    validateTargetTable(mTable);

    ASTNode where = null;
    int whereIndex = 2;
    if (children.size() > whereIndex) {
      where = (ASTNode) children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
              "Expected where clause, but found " + where.getName();
    }

    // Must be deterministic order set for consistent q-test output across Java versions
    Set<String> setRCols = new LinkedHashSet<String>();
    // We won't write the set
    // expressions in the rewritten query.  We'll patch that up later.
    // The set list from update should be the second child (index 1)
    assert children.size() >= 2 : "Expected update token to have at least two children";
    ASTNode setClause = (ASTNode) children.get(1);
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(setClause, setRCols, mTable);
    Map<Integer, ASTNode> setColExprs = new HashMap<>(setClause.getChildCount());

    StringBuilder selectExpressions = new StringBuilder();
    StringBuilder aliasedSelectExpressions = new StringBuilder();
    List<FieldSchema> nonPartCols = mTable.getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      if (i != 0) {
        selectExpressions.append(',');
        aliasedSelectExpressions.append(',');
      }
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = setCols.get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);
      selectExpressions.append(identifier);
      aliasedSelectExpressions.append("s.");
      aliasedSelectExpressions.append(identifier);
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(i + 1, setCol);
      }
    }

    StringBuilder rewrittenQueryStr = new StringBuilder();

    rewrittenQueryStr.append("FROM (SELECT ");
    rewrittenQueryStr.append(selectExpressions);
    rewrittenQueryStr.append(" FROM ");
    rewrittenQueryStr.append(getFullTableNameForSQL(tabName));
    rewrittenQueryStr.append(") s\n");

    // First insert branch for insert new values
    rewrittenQueryStr.append("INSERT INTO ");
    rewrittenQueryStr.append(getFullTableNameForSQL(tabName));
    rewrittenQueryStr.append(" SELECT ");
    rewrittenQueryStr.append(aliasedSelectExpressions);
    rewrittenQueryStr.append("\n");

    // Second insert branch for delete old values
    rewrittenQueryStr.append("INSERT INTO ");
    rewrittenQueryStr.append(getFullTableNameForSQL(tabName));
    rewrittenQueryStr.append(" SELECT ROW__ID SORT BY ROW__ID");

    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode) rewrittenTree.getChildren().get(1);
    assert rewrittenInsert.getToken().getType() == HiveParser.TOK_INSERT :
            "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName();

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);

    if (where != null) {
      // The structure of the AST for the rewritten insert statement is:
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //                        \-> TOK_SORTBY
      // The following adds the TOK_WHERE and its subtree from the original query as a child of
      // TOK_INSERT, which is where it would have landed if it had been there originally in the
      // string.  We do it this way because it's easy then turning the original AST back into a
      // string and reparsing it.  We have to move the SORT_BY over one,
      // so grab it and then push it to the second slot, and put the where in the first slot
      ASTNode sortBy = (ASTNode) rewrittenInsert.getChildren().get(2);
      assert sortBy.getToken().getType() == HiveParser.TOK_SORTBY :
              "Expected TOK_SORTBY to be first child of TOK_SELECT, but found " + sortBy.getName();
      rewrittenInsert.addChild(sortBy);
      rewrittenInsert.setChild(2, where);
    }

    // Patch up the projection list for updates, putting back the original set expressions.
    // Walk through the projection list and replace the column names with the
    // expressions from the original update.  Under the TOK_SELECT (see above) the structure
    // looks like:
    // TOK_SELECT -> TOK_SELEXPR -> expr
    //           \-> TOK_SELEXPR -> expr ...
    ASTNode rewrittenSelect = (ASTNode) rewrittenInsert.getChildren().get(1);
    assert rewrittenSelect.getToken().getType() == HiveParser.TOK_SELECT :
            "Expected TOK_SELECT as second child of TOK_INSERT but found " +
                    rewrittenSelect.getName();
    for (Map.Entry<Integer, ASTNode> entry : setColExprs.entrySet()) {
      ASTNode selExpr = (ASTNode) rewrittenSelect.getChildren().get(entry.getKey());
      assert selExpr.getToken().getType() == HiveParser.TOK_SELEXPR :
              "Expected child of TOK_SELECT to be TOK_SELEXPR but was " + selExpr.getName();
      // Now, change it's child
      selExpr.setChild(0, entry.getValue());
    }

    try {
      useSuper = true;
      // Note: this will overwrite this.ctx with rewrittenCtx
      rewrittenCtx.setEnableUnparse(false);
      super.analyze(rewrittenTree, rewrittenCtx);
    } finally {
      useSuper = false;
    }

    updateOutputs(mTable);


    setUpAccessControlInfoForUpdate(mTable, setCols);

    // Add the setRCols to the input list
    for (String colName : setRCols) {
      if (columnAccessInfo != null) { //assuming this means we are not doing Auth
        columnAccessInfo.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()),
                colName);
      }
    }
  }
}
