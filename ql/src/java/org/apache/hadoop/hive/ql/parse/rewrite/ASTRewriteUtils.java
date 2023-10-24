package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ASTRewriteUtils {
  private ASTRewriteUtils() {}

  public static void addPartitionColsAsValues(List<FieldSchema> partCols, String alias, List<String> values) {
    if (partCols == null) {
      return;
    }
    partCols.forEach(
        fieldSchema -> values.add(alias + "." + HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf)));
  }
}
