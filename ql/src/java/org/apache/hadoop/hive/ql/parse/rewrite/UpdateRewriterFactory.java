package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;

import static org.apache.hadoop.hive.ql.parse.rewrite.SqlBuilderFactory.DELETE_PREFIX;
import static org.apache.hadoop.hive.ql.parse.rewrite.SqlBuilderFactory.SUB_QUERY_ALIAS;

public class UpdateRewriterFactory implements RewriterFactory<UpdateSemanticAnalyzer.UpdateBlock> {
  protected final HiveConf conf;

  public UpdateRewriterFactory(HiveConf conf) {
    this.conf = conf;
  }

  public Rewriter<UpdateSemanticAnalyzer.UpdateBlock> createRewriter(Table table, String targetTableFullName) {
    boolean splitUpdate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.SPLIT_UPDATE);
    boolean copyOnWriteMode = false;
    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null) {
      copyOnWriteMode = storageHandler.shouldOverwrite(table, Context.Operation.UPDATE);
    }

    SqlBuilderFactory sqlBuilderFactory = new SqlBuilderFactory(
        table, targetTableFullName, conf, splitUpdate && !copyOnWriteMode ? SUB_QUERY_ALIAS : null, DELETE_PREFIX);

    if (copyOnWriteMode) {
      return new CopyOnWriteUpdateRewriter(conf, sqlBuilderFactory);
    } else if (splitUpdate) {
      return new SplitUpdateRewriter(conf, sqlBuilderFactory);
    } else {
      return new UpdateRewriter(conf, sqlBuilderFactory);
    }
  }
}
