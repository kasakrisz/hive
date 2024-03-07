package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

public class SnapshotRowCountSource implements RowCountSource{
  private final org.apache.hadoop.hive.ql.metadata.Table table;

  public SnapshotRowCountSource(org.apache.hadoop.hive.ql.metadata.Table table) {
    this.table = table;
  }

  @Override
  public double getInsertedCount() {
    return table.getInsertedCount();
  }
}
