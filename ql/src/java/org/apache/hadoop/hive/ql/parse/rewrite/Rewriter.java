package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public interface Rewriter<T> {
  ParseUtils.ReparseResult rewrite(Context context, T rewriteData) throws SemanticException;
}
