package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.ql.metadata.Table;

public class RewrittenSqlBuilder {

  static RewrittenSqlBuilder with(Table table, String targetTableFullName) {
    return new RewrittenSqlBuilder(table, targetTableFullName);
  }

  private final Table targetTable;
  private final String targetTableFullName;


  public RewrittenSqlBuilder(Table targetTable, String targetTableFullName) {
    this.targetTable = targetTable;
    this.targetTableFullName = targetTableFullName;
  }
}
