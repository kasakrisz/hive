package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NonNativeAcidMultiInsertSqlBuilder extends MultiInsertSqlBuilder {
  private final String deletePrefix;

  public NonNativeAcidMultiInsertSqlBuilder(
      Table table, String targetTableFullName, HiveConf conf, String subQueryAlias, String deletePrefix) {
    super(table, targetTableFullName, conf, subQueryAlias);
    this.deletePrefix = deletePrefix;
  }

  @Override
  public void appendAcidSelectColumns(Context.Operation operation) {
    appendAcidSelectColumns(operation, false);
  }

  @Override
  public void appendAcidSelectColumnsForDeletedRecords(Context.Operation operation) {
    appendAcidSelectColumns(operation, true);
  }

  private void appendAcidSelectColumns(Context.Operation operation, boolean markRowIdAsDeleted) {
    List<FieldSchema> acidSelectColumns = targetTable.getStorageHandler().acidSelectColumns(targetTable, operation);
    for (FieldSchema fieldSchema : acidSelectColumns) {
      String identifier = markRowIdAsDeleted && fieldSchema.equals(targetTable.getStorageHandler().getRowId()) ?
          "-1" : HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf);
      rewrittenQueryStr.append(identifier);

      if (StringUtils.isNotEmpty(deletePrefix) && !markRowIdAsDeleted) {
        rewrittenQueryStr.append(" AS ");
        String prefixedIdentifier = HiveUtils.unparseIdentifier(deletePrefix + fieldSchema.getName(), this.conf);
        rewrittenQueryStr.append(prefixedIdentifier);
      }
      rewrittenQueryStr.append(",");
    }
  }

  @Override
  public List<String> getDeleteValues(Context.Operation operation) {
    List<FieldSchema> acidSelectColumns = targetTable.getStorageHandler().acidSelectColumns(targetTable, operation);
    List<String> deleteValues = new ArrayList<>(acidSelectColumns.size());
    for (FieldSchema fieldSchema : acidSelectColumns) {
      String prefixedIdentifier = HiveUtils.unparseIdentifier(deletePrefix + fieldSchema.getName(), this.conf);
      deleteValues.add(qualify(prefixedIdentifier));
    }
    return deleteValues;
  }

  @Override
  public List<String> getSortKeys() {
    return targetTable.getStorageHandler().acidSortColumns(targetTable, Context.Operation.DELETE).stream()
        .map(fieldSchema -> qualify(
            HiveUtils.unparseIdentifier(deletePrefix + fieldSchema.getName(), this.conf)))
        .collect(Collectors.toList());
  }
}
