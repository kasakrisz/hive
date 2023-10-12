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
    sqlBuilder.addPartitionColsToInsert(deleteBlock.getTargetTable().getPartCols());

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
