package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;

import static org.apache.hadoop.hive.ql.parse.rewrite.SqlBuilderFactory.DELETE_PREFIX;

public class DeleteRewriterFactory implements RewriterFactory<DeleteSemanticAnalyzer.DeleteBlock> {
  protected final HiveConf conf;

  public DeleteRewriterFactory(HiveConf conf) {
    this.conf = conf;
  }

  public Rewriter<DeleteSemanticAnalyzer.DeleteBlock> createRewriter(Table table, String targetTableFullName) {
    boolean copyOnWriteMode = false;
    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null) {
      copyOnWriteMode = storageHandler.shouldOverwrite(table, Context.Operation.DELETE);
    }

    SqlBuilderFactory sqlBuilderFactory = new SqlBuilderFactory(
        table, targetTableFullName, conf, null, DELETE_PREFIX);

    if (copyOnWriteMode) {
      return new CopyOnWriteDeleteRewriter(conf, sqlBuilderFactory);
    } else {
      return new DeleteRewriter(sqlBuilderFactory);
    }
  }
}
