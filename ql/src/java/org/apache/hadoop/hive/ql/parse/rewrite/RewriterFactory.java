package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.ql.metadata.Table;

public interface RewriterFactory<T> {
  Rewriter<T> createRewriter(Table table, String targetTableFullName);
}
