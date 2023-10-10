package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class UpdateSemanticAnalyzer extends RewriteSemanticAnalyzer2 {
  public static final String DELETE_PREFIX = "__d__";
  public static final String SUB_QUERY_ALIAS = "s";

  private Context.Operation operation = Context.Operation.OTHER;

  UpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode) tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + operation + " but found " + tabName.getName();
    return tabName;
  }

  @Override
  protected void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException {
    boolean splitUpdate = HiveConf.getBoolVar(queryState.getConf(), HiveConf.ConfVars.SPLIT_UPDATE);
    MultiInsertSqlBuilder multiInsertSqlBuilder = getColumnAppender(SUB_QUERY_ALIAS, DELETE_PREFIX);
    Rewriter rewriter;
    if (splitUpdate) {
      rewriter = new SplitUpdateRewriter(conf, multiInsertSqlBuilder);
    } else {
      rewriter = new UpdateRewriter();
    }

    ParseUtils.ReparseResult rr = rewriter.rewrite(ctx, tree, table);

    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

    updateOutputs(table);

    setUpAccessControlInfoForUpdate(table, setCols);

    // Add the setRCols to the input list
    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
      return;
    }

    for (String colName : setRCols) {
      columnAccessInfo.add(Table.getCompleteName(table.getDbName(), table.getTableName()), colName);
    }
  }
}
