package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

public class SqlBuilderFactory {
  public static final String DELETE_PREFIX = "__d__";
  public static final String SUB_QUERY_ALIAS = "s";

  private final Table targetTable;
  private final String targetTableFullName;
  private final HiveConf conf;
  private final String subQueryAlias;
  private final String deletePrefix;

  public SqlBuilderFactory(
      Table targetTable, String targetTableFullName, HiveConf conf, String subQueryAlias, String deletePrefix) {
    this.targetTable = targetTable;
    this.targetTableFullName = targetTableFullName;
    this.conf = conf;
    this.subQueryAlias = subQueryAlias;
    this.deletePrefix = deletePrefix;
  }

  public MultiInsertSqlBuilder createSqlBuilder() {
    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(targetTable, true);
    return nonNativeAcid ? new NonNativeAcidMultiInsertSqlBuilder(targetTable, targetTableFullName, conf, subQueryAlias, deletePrefix) :
        new NativeAcidMultiInsertSqlBuilder(targetTable, targetTableFullName, conf, subQueryAlias);
  }
}
