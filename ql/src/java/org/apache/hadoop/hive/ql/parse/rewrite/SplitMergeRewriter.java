package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.ql.Context;

import java.util.List;

public class SplitMergeRewriter extends MergeRewriter {
  public SplitMergeRewriter(MultiInsertSqlBuilder sqlBuilder) {
    super(sqlBuilder);
  }

  public void handleWhenMatchedUpdate(String targetAlias, List<String> values, String hintStr, String onClauseAsString,
                                      String extraPredicate, String deleteExtraPredicate) {
    sqlBuilder.append("    -- update clause (insert part)\n");
    sqlBuilder.appendInsertBranch(hintStr, values);

    addWhereClauseOfUpdate(onClauseAsString, extraPredicate, deleteExtraPredicate);

    sqlBuilder.append("\n");

    sqlBuilder.append("    -- update clause (delete part)\n");
    handleWhenMatchedDelete(hintStr, onClauseAsString, deleteExtraPredicate, extraPredicate);
  }

  @Override
  public int addDestNamePrefixOfUpdate(int insClauseIdx, Context rewrittenCtx) {
    rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(insClauseIdx + 1, Context.DestClausePrefix.DELETE);
    return 2;
  }
}
