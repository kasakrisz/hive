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
import org.apache.hadoop.hive.ql.session.SessionStateUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A subclass of the {@link SemanticAnalyzer} that just handles
 * merge statements. It works by rewriting the updates and deletes into insert statements (since
 * they are actually inserts) and then doing some patch up to make them work as merges instead.
 */
public class SplitUpdateSemanticAnalyzer extends SplitRewriteSemanticAnalyzer {

  private Context.Operation operation = Context.Operation.OTHER;

  SplitUpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyze(ASTNode tree) throws SemanticException {
    if (tree.getToken().getType() != HiveParser.TOK_UPDATE) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
              "SplitUpdateSemanticAnalyzer");
    }
    ctx.setOperation(Context.Operation.UPDATE);
    analyzeUpdate(tree);
  }

  private void analyzeUpdate(ASTNode tree) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

//    TOK_UPDATE_TABLE
//            TOK_TABNAME <- The first child should be the table we are updating
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

    Set<String> setRCols = new LinkedHashSet<>();
//    TOK_UPDATE_TABLE
//            TOK_TABNAME
//               ...
//            TOK_SET_COLUMNS_CLAUSE <- The set list from update should be the second child (index 1)
    assert children.size() >= 2 : "Expected update token to have at least two children";
    ASTNode setClause = (ASTNode) children.get(1);
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(setClause, setRCols, mTable);
    Map<Integer, ASTNode> setColExprs = new HashMap<>(setClause.getChildCount());

    List<FieldSchema> nonPartCols = mTable.getCols();
    List<String> values = new ArrayList<>();
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(mTable);
    for (int i = 0; i < nonPartCols.size(); i++) {
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = setCols.get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);

      if (setCol != null) {
        if (setCol.getChildCount() > 0 && "default".equalsIgnoreCase(setCol.getChild(0).getText())) {
          values.add(colNameToDefaultConstraint.get(name));
        } else {
          values.add(identifier);
          selectExpressions.append(identifier);
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up. 0th is ROW_ID
          setColExprs.put(i + 1, setCol);
        }
      } else {
        selectExpressions.append(identifier);
      }
      selectExpressions.append(" AS ");
      selectExpressions.append(identifier);

      aliasedSelectExpressions.append("s.");
      aliasedSelectExpressions.append(identifier);
    }
    addPartitionColsToSelect(mTable.getPartCols(), selectExpressions);
    addPartitionColsToSelect(mTable.getPartCols(), aliasedSelectExpressions, "s");

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
    rewrittenQueryStr.append(" SELECT s.ROW__ID ");
    addPartitionColsToSelect(mTable.getPartCols(), rewrittenQueryStr, "s");
    rewrittenQueryStr.append(" SORT BY s.ROW__ID");

    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = new ASTSearcher().simpleBreadthFirstSearch(
            rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_INSERT);

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(2, Context.DestClausePrefix.DELETE);

    if (where != null) {
      rewrittenInsert.addChild(where);
    }

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

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
