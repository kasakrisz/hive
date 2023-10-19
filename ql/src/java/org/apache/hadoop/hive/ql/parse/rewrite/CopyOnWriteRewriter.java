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
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CopyOnWriteRewriter extends DeleteRewriter {

  private final HiveConf conf;

  public CopyOnWriteRewriter(HiveConf conf, MultiInsertSqlBuilder sqlBuilder) {
    super(sqlBuilder);
    this.conf = conf;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, DeleteSemanticAnalyzer.DeleteBlock deleteBlock)
      throws SemanticException {

    String whereClause = context.getTokenRewriteStream().toString(
        deleteBlock.getWhereTree().getChild(0).getTokenStartIndex(),
        deleteBlock.getWhereTree().getChild(0).getTokenStopIndex());
    String filePathCol = HiveUtils.unparseIdentifier("FILE__PATH", conf);

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
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());
    sqlBuilder.appendPartitionCols(deleteBlock.getTargetTable().getPartCols());

    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.DELETE);
    sqlBuilder.removeLastChar();

    sqlBuilder.append(" from ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());

    // Add the inverted where clause, since we want to hold the records which doesn't satisfy the condition.
    sqlBuilder.append("\nwhere NOT (").append(whereClause).append(")");
    sqlBuilder.append("\n");
    // Add the file path filter that matches the delete condition.
    sqlBuilder.append("AND ").append(filePathCol);
    sqlBuilder.append(" IN ( select ").append(filePathCol).append(" from t )");
    sqlBuilder.append("\nunion all");
    sqlBuilder.append("\nselect * from t");

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;

    rewrittenCtx.setOperation(Context.Operation.DELETE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
