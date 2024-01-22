package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

public enum IncrementalRebuildMode {
  AVAILABLE,
  INSERT_ONLY,
  NOT_AVAILABLE,
  UNKNOWN
}
