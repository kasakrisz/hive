package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyOnWriteRewriter extends UpdateRewriter {

  public CopyOnWriteRewriter(HiveConf conf, MultiInsertSqlBuilder sqlBuilder) {
    super(conf, sqlBuilder);
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateSemanticAnalyzer.UpdateBlock updateBlock)
      throws SemanticException {

    String whereClause = context.getTokenRewriteStream().toString(
        updateBlock.getWhereTree().getChild(0).getTokenStartIndex(),
        updateBlock.getWhereTree().getChild(0).getTokenStopIndex());
    String filePathCol = HiveUtils.unparseIdentifier("FILE__PATH", conf);

    sqlBuilder.append("WITH t AS (");
    sqlBuilder.append("\n");
    sqlBuilder.append("select ");
    sqlBuilder.appendAcidSelectColumnsForDeletedRecords(Context.Operation.UPDATE);
    sqlBuilder.removeLastChar();
    sqlBuilder.append(" from (");
    sqlBuilder.append("\n");
    sqlBuilder.append("select ");
    sqlBuilder.appendAcidSelectColumnsForDeletedRecords(Context.Operation.UPDATE);
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
    sqlBuilder.addPartitionColsToInsert(updateBlock.getTargetTable().getPartCols());

    int columnOffset = sqlBuilder.getDeleteValues(Context.Operation.UPDATE).size();
    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.UPDATE);
    sqlBuilder.removeLastChar();

    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetCols().size());

    // Must be deterministic order set for consistent q-test output across Java versions
    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      sqlBuilder.append(",");
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      sqlBuilder.append(HiveUtils.unparseIdentifier(name, this.conf));
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(columnOffset + i, setCol);
      }
    }

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
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode)rewrittenTree.getChildren().get(1);
    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
