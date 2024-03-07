package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.hadoop.hive.metastore.api.SourceTable;

public class SourceTableRowCountSource implements RowCountSource{
  private final SourceTable sourceTable;

  public SourceTableRowCountSource(SourceTable sourceTable) {
    this.sourceTable = sourceTable;
  }

  @Override
  public double getInsertedCount() {
    return sourceTable.getInsertedCount();
  }
}
