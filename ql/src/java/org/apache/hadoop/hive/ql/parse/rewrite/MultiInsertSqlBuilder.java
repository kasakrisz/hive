package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.ql.parse.rewrite.RewriteSemanticAnalyzer2.INDENT;

public abstract class MultiInsertSqlBuilder {
  protected final Table targetTable;
  protected final String targetTableFullName;
  protected final HiveConf conf;
  protected final String subQueryAlias;
  protected final StringBuilder rewrittenQueryStr;

  protected MultiInsertSqlBuilder(Table targetTable, String targetTableFullName, HiveConf conf, String subQueryAlias) {
    this.targetTable = targetTable;
    this.targetTableFullName = targetTableFullName;
    this.conf = conf;
    this.subQueryAlias = subQueryAlias;
    this.rewrittenQueryStr = new StringBuilder();
  }

  public Table getTargetTable() {
    return targetTable;
  }

  public String getTargetTableFullName() {
    return targetTableFullName;
  }

  public abstract void appendAcidSelectColumns(Context.Operation operation);

  public void appendAcidSelectColumnsForDeletedRecords(Context.Operation operation) {
    throw new UnsupportedOperationException();
  }

  public abstract List<String> getDeleteValues(Context.Operation operation);
  public abstract List<String> getSortKeys();

  protected String qualify(String columnName) {
    if (isBlank(subQueryAlias)) {
      return columnName;
    }
    return String.format("%s.%s", subQueryAlias, columnName);
  }

  public void appendInsertBranch(String hintStr, List<String> values) {
    rewrittenQueryStr.append("INSERT INTO ").append(targetTableFullName);
    addPartitionColsToInsert(targetTable.getPartCols());
    rewrittenQueryStr.append("\n");

    rewrittenQueryStr.append(INDENT);
    rewrittenQueryStr.append("SELECT ");
    if (isNotBlank(hintStr)) {
      rewrittenQueryStr.append(hintStr);
    }

    rewrittenQueryStr.append(StringUtils.join(values, ","));
    rewrittenQueryStr.append("\n");
  }

  /**
   * Append list of partition columns to Insert statement, i.e. the 1st set of partCol1,partCol2
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   */
  public void addPartitionColsToInsert(List<FieldSchema> partCols) {
    addPartitionColsToInsert(partCols, null);
  }

  /**
   * Append list of partition columns to Insert statement. If user specified partition spec, then
   * use it to get/set the value for partition column else use dynamic partition mode with no value.
   * Static partition mode:
   * INSERT INTO T PARTITION(partCol1=val1,partCol2...) SELECT col1, ... partCol1,partCol2...
   * Dynamic partition mode:
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   */
  protected void addPartitionColsToInsert(List<FieldSchema> partCols, Map<String, String> partSpec) {
    // If the table is partitioned we have to put the partition() clause in
    if (partCols != null && !partCols.isEmpty()) {
      rewrittenQueryStr.append(" partition (");
      boolean first = true;
      for (FieldSchema fschema : partCols) {
        if (first) {
          first = false;
        } else {
          rewrittenQueryStr.append(", ");
        }
        // Would be nice if there was a way to determine if quotes are needed
        rewrittenQueryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
        String partVal = (partSpec != null) ? partSpec.get(fschema.getName()) : null;
        if (partVal != null) {
          rewrittenQueryStr.append("=").append(partVal);
        }
      }
      rewrittenQueryStr.append(")");
    }
  }

  void appendSortBy(List<String> keys) {
    if (keys.isEmpty()) {
      return;
    }
    rewrittenQueryStr.append(INDENT).append("SORT BY ");
    rewrittenQueryStr.append(StringUtils.join(keys, ","));
    rewrittenQueryStr.append("\n");
  }

  public MultiInsertSqlBuilder append(String sqlTextFragment) {
    rewrittenQueryStr.append(sqlTextFragment);
    return this;
  }

  @Override
  public String toString() {
    return rewrittenQueryStr.toString();
  }

  public void removeLastChar() {
    rewrittenQueryStr.setLength(rewrittenQueryStr.length() - 1);
  }
}
