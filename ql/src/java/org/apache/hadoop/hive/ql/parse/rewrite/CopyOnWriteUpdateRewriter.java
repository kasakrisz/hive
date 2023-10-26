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
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlBuilderFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyOnWriteUpdateRewriter extends UpdateRewriter {

  public CopyOnWriteUpdateRewriter(HiveConf conf, SqlBuilderFactory sqlBuilderFactory) {
    super(conf, sqlBuilderFactory);
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateSemanticAnalyzer.UpdateBlock updateBlock)
      throws SemanticException {

    String whereClause = context.getTokenRewriteStream().toString(
        updateBlock.getWhereTree().getChild(0).getTokenStartIndex(),
        updateBlock.getWhereTree().getChild(0).getTokenStopIndex());
    String filePathCol = HiveUtils.unparseIdentifier("FILE__PATH", conf);

    MultiInsertSqlBuilder sqlBuilder = sqlBuilderFactory.createSqlBuilder();

    sqlBuilder.append("WITH t AS (");
    sqlBuilder.append("\n");
    sqlBuilder.append("select ");
    sqlBuilder.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE);
    sqlBuilder.removeLastChar();
    sqlBuilder.append(" from (");
    sqlBuilder.append("\n");
    sqlBuilder.append("select ");
    sqlBuilder.appendAcidSelectColumnsForDeletedRecords(Context.Operation.DELETE);
    sqlBuilder.append(" row_number() OVER (partition by ").append(filePathCol).append(") rn");
    sqlBuilder.append(" from ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());
    sqlBuilder.append("\n");
    sqlBuilder.append("where ").append(whereClause);
    sqlBuilder.append("\n");
    sqlBuilder.append(") q");
    sqlBuilder.append("\n");
    sqlBuilder.append("where rn=1\n)\n");

    sqlBuilder.append("insert into table ");
    sqlBuilder.appendTargetTableName();
    sqlBuilder.appendPartitionColsOfTarget();

    int columnOffset = sqlBuilder.getDeleteValues(Context.Operation.UPDATE).size();
    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.UPDATE);
    sqlBuilder.removeLastChar();

    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetCols().size());
    // Must be deterministic order set for consistent q-test output across Java versions
    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      sqlBuilder.append(',');
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);
      sqlBuilder.append(identifier);
      sqlBuilder.append(" AS ").append(identifier);
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(columnOffset + i, setCol);
      }
    }

    sqlBuilder.append(" from ");
    sqlBuilder.appendTargetTableName();

    if (updateBlock.getWhereTree() != null) {

      sqlBuilder.append("\nunion all");
      sqlBuilder.append("\nselect ");
      sqlBuilder.appendAcidSelectColumns(Context.Operation.DELETE);
      sqlBuilder.removeLastChar();
      sqlBuilder.append(" from ");
      sqlBuilder.appendTargetTableName();
      // Add the inverted where clause, since we want to hold the records which doesn't satisfy the condition.
      sqlBuilder.append("\nwhere NOT (").append(whereClause).append(")");
      sqlBuilder.append("\n").indent();
      // Add the file path filter that matches the delete condition.
      sqlBuilder.append("AND ").append(filePathCol);
      sqlBuilder.append(" IN ( select ").append(filePathCol).append(" from t )");
      sqlBuilder.append("\nunion all");
      sqlBuilder.append("\nselect * from t");
    }

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode) new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_UNIONALL).getChild(0).getChild(0)
        .getChild(1);

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);

    if (updateBlock.getWhereTree() != null) {
      assert rewrittenInsert.getToken().getType() == HiveParser.TOK_INSERT :
          "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName();
      // The structure of the AST for the rewritten insert statement is:
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //                        \-> TOK_SORTBY
      // Or
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //
      // The following adds the TOK_WHERE and its subtree from the original query as a child of
      // TOK_INSERT, which is where it would have landed if it had been there originally in the
      // string.  We do it this way because it's easy then turning the original AST back into a
      // string and reparsing it.
      if (rewrittenInsert.getChildren().size() == 3) {
        // We have to move the SORT_BY over one, so grab it and then push it to the second slot,
        // and put the where in the first slot
        ASTNode sortBy = (ASTNode) rewrittenInsert.getChildren().get(2);
        assert sortBy.getToken().getType() == HiveParser.TOK_SORTBY :
            "Expected TOK_SORTBY to be third child of TOK_INSERT, but found " + sortBy.getName();
        rewrittenInsert.addChild(sortBy);
        rewrittenInsert.setChild(2, updateBlock.getWhereTree());
      } else {
        ASTNode select = (ASTNode) rewrittenInsert.getChildren().get(1);
        assert select.getToken().getType() == HiveParser.TOK_SELECT :
            "Expected TOK_SELECT to be second child of TOK_INSERT, but found " + select.getName();
        rewrittenInsert.addChild(updateBlock.getWhereTree());
      }
    }

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
