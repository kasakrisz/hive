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
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * A subclass of the {@link SemanticAnalyzer} that just handles
 * update statements. It works by rewriting the updates into multi-insert
 * statements (since they are actually inserts) with two insert branches: one for inserting new values
 * and another for deleting old values.
 */
public class SplitUpdateSemanticAnalyzer extends RewriteSemanticAnalyzer {

  SplitUpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode)tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
            "Expected tablename as first child of Update but found " + tabName.getName();
    return tabName;
  }

  protected void analyze(ASTNode tree, Table targetTable, ASTNode tabNameNode) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

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
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(setClause, setRCols, targetTable);
    Map<Integer, ASTNode> setColExprs = new HashMap<>(setClause.getChildCount());

    List<FieldSchema> nonPartCols = targetTable.getCols();
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(targetTable);
    StringBuilder rewrittenQueryStr = createRewrittenQueryStrBuilder();
    rewrittenQueryStr.append("(SELECT ");

    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(targetTable);
    int columnOffset;
    List<String> deleteValues;
    if (nonNativeAcid) {
      List<FieldSchema> acidSelectColumns =
          targetTable.getStorageHandler().acidSelectColumns(targetTable, Context.Operation.UPDATE);
      deleteValues = new ArrayList<>(acidSelectColumns.size());
      for (FieldSchema fieldSchema : acidSelectColumns) {
        String identifier = HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf);
        rewrittenQueryStr.append(identifier).append(" AS ");
        String prefixedIdentifier = HiveUtils.unparseIdentifier(DELETE_PREFIX + fieldSchema.getName(), this.conf);
        rewrittenQueryStr.append(prefixedIdentifier);
        rewrittenQueryStr.append(",");
        deleteValues.add(String.format("%s.%s", SUB_QUERY_ALIAS, prefixedIdentifier));
      }

      columnOffset = acidSelectColumns.size();
    } else {
      rewrittenQueryStr.append("ROW__ID,");
      deleteValues = new ArrayList<>(1 + targetTable.getPartCols().size());
      deleteValues.add(SUB_QUERY_ALIAS + ".ROW__ID");
      for (FieldSchema fieldSchema : targetTable.getPartCols()) {
        deleteValues.add(SUB_QUERY_ALIAS + "." + HiveUtils.unparseIdentifier(fieldSchema.getName(), conf));
      }
      columnOffset = 1;
    }

    List<String> insertValues = new ArrayList<>(targetTable.getCols().size());
    boolean first = true;

    for (int i = 0; i < nonPartCols.size(); i++) {
      if (first) {
        first = false;
      } else {
        rewrittenQueryStr.append(",");
      }

      String name = nonPartCols.get(i).getName();
      ASTNode setCol = setCols.get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);

      if (setCol != null) {
        if (setCol.getType() == HiveParser.TOK_TABLE_OR_COL &&
                setCol.getChildCount() == 1 && setCol.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          rewrittenQueryStr.append(colNameToDefaultConstraint.get(name));
        } else {
          rewrittenQueryStr.append(identifier);
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up. 0th is ROW_ID
          setColExprs.put(i + columnOffset, setCol);
        }
      } else {
        rewrittenQueryStr.append(identifier);
      }
      rewrittenQueryStr.append(" AS ");
      rewrittenQueryStr.append(identifier);

      insertValues.add(SUB_QUERY_ALIAS + "." + identifier);
    }
    addColsToSelect(targetTable.getPartCols(), rewrittenQueryStr);
    addPartitionColsAsValues(targetTable.getPartCols(), SUB_QUERY_ALIAS, insertValues);
    rewrittenQueryStr.append(" FROM ").append(getFullTableNameForSQL(tabNameNode)).append(") ");
    rewrittenQueryStr.append(SUB_QUERY_ALIAS).append("\n");

    appendInsertBranch(rewrittenQueryStr, null, insertValues);
    appendInsertBranch(rewrittenQueryStr, null, deleteValues);

    List<String> sortKeys;
    if (nonNativeAcid) {
      sortKeys = targetTable.getStorageHandler().acidSortColumns(targetTable, Context.Operation.DELETE).stream()
              .map(fieldSchema -> String.format(
                      "%s.%s",
                      SUB_QUERY_ALIAS,
                      HiveUtils.unparseIdentifier(DELETE_PREFIX + fieldSchema.getName(), this.conf)))
              .collect(Collectors.toList());
    } else {
      sortKeys = singletonList(SUB_QUERY_ALIAS + ".ROW__ID ");
    }
    appendSortBy(rewrittenQueryStr, sortKeys);

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

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

    updateOutputs(targetTable);

    setUpAccessControlInfoForUpdate(targetTable, setCols);

    // Add the setRCols to the input list
    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
      return;
    }

    for (String colName : setRCols) {
      columnAccessInfo.add(Table.getCompleteName(targetTable.getDbName(), targetTable.getTableName()), colName);
    }
  }

  @Override
  protected boolean allowOutputMultipleTimes() {
    return true;
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }

  @Override
  protected void checkPartitionAndBucketColsInSetClauseTarget(
      String columnName, Table targetTable) throws SemanticException {
    // Make sure this isn't one of the partitioning columns, that's not supported.
    for (FieldSchema fschema : targetTable.getPartCols()) {
      if (fschema.getName().equalsIgnoreCase(columnName)) {
        throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_PART_VALUE.getMsg());
      }
    }
  }
}
