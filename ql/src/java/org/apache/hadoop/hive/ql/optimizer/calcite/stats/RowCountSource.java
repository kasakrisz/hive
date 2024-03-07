package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

public interface RowCountSource {
  double getInsertedCount();
}
