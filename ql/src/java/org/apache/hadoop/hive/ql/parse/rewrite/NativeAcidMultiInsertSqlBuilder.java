package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public class NativeAcidMultiInsertSqlBuilder extends MultiInsertSqlBuilder {
  public NativeAcidMultiInsertSqlBuilder(Table table, String targetTableFullName, HiveConf conf, String subQueryAlias) {
    super(table, targetTableFullName, conf, subQueryAlias);
  }

  @Override
  public void appendAcidSelectColumns(Context.Operation operation) {
    rewrittenQueryStr.append("ROW__ID,");
    for (FieldSchema fieldSchema : targetTable.getPartCols()) {
      String identifier = HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf);
      rewrittenQueryStr.append(identifier);
      rewrittenQueryStr.append(",");
    }
  }

  @Override
  public List<String> getDeleteValues(Context.Operation operation) {
    List<String> deleteValues = new ArrayList<>(1 + targetTable.getPartCols().size());
    deleteValues.add(qualify("ROW__ID"));
    for (FieldSchema fieldSchema : targetTable.getPartCols()) {
      deleteValues.add(qualify(HiveUtils.unparseIdentifier(fieldSchema.getName(), conf)));
    }
    return deleteValues;
  }

  @Override
  public List<String> getSortKeys() {
    return singletonList(qualify("ROW__ID"));
  }
}
