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
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitUpdateRewriter extends UpdateRewriter {

  private final Context.Operation operation = Context.Operation.UPDATE;

  private final HiveConf conf;

  public SplitUpdateRewriter(HiveConf conf, SqlBuilderFactory sqlBuilderFactory) {
    super(conf, sqlBuilderFactory);
    this.conf = conf;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateSemanticAnalyzer.UpdateBlock updateBlock)
      throws SemanticException {
    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetClauseTree().getChildCount());

    MultiInsertSqlBuilder sqlBuilder = sqlBuilderFactory.createSqlBuilder();

    sqlBuilder.append("FROM\n");
    sqlBuilder.append("(SELECT ");

    sqlBuilder.appendAcidSelectColumns(operation);
    List<String> deleteValues = sqlBuilder.getDeleteValues(operation);
    int columnOffset = deleteValues.size();

    List<String> insertValues = new ArrayList<>(updateBlock.getTargetTable().getCols().size());
    boolean first = true;

    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      if (first) {
        first = false;
      } else {
        sqlBuilder.append(",");
      }

      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);

      if (setCol != null) {
        if (setCol.getType() == HiveParser.TOK_TABLE_OR_COL &&
            setCol.getChildCount() == 1 && setCol.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          sqlBuilder.append(updateBlock.getColNameToDefaultConstraint().get(name));
        } else {
          sqlBuilder.append(identifier);
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up. 0th is ROW_ID
          setColExprs.put(i + columnOffset, setCol);
        }
      } else {
        sqlBuilder.append(identifier);
      }
      sqlBuilder.append(" AS ");
      sqlBuilder.append(identifier);

      insertValues.add(sqlBuilder.subQueryAlias + "." + identifier);
    }
    addPartitionColsAsValues(updateBlock.getTargetTable().getPartCols(), sqlBuilder.subQueryAlias, insertValues, conf);
    sqlBuilder.append(" FROM ").append(sqlBuilder.getTargetTableFullName()).append(") ");
    sqlBuilder.appendSubQueryAlias().append("\n");

    sqlBuilder.appendInsertBranch(null, insertValues);
    sqlBuilder.appendInsertBranch(null, deleteValues);

    List<String> sortKeys = sqlBuilder.getSortKeys();
    sqlBuilder.appendSortBy(sortKeys);

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
        rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_INSERT);

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(2, Context.DestClausePrefix.DELETE);

    if (updateBlock.getWhereTree() != null) {
      rewrittenInsert.addChild(updateBlock.getWhereTree());
    }

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);

    return rr;
  }

  public static void addPartitionColsAsValues(
      List<FieldSchema> partCols, String alias, List<String> values, HiveConf conf) {
    if (partCols == null) {
      return;
    }
    partCols.forEach(
        fieldSchema -> values.add(alias + "." + HiveUtils.unparseIdentifier(fieldSchema.getName(), conf)));
  }
}
