package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public interface Rewriter {
  ParseUtils.ReparseResult rewrite(Context context, ASTNode tree, Table table) throws SemanticException;
}
