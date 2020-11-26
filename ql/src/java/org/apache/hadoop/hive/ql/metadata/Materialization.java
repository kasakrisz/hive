package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.RelOptMaterialization;

public class Materialization {

  public enum RewriteAlgorithm {
    TEXT,
    ALL
  }

  private final RelOptMaterialization relOptMaterialization;
  private final RewriteAlgorithm scope;

  public Materialization(RelOptMaterialization relOptMaterialization, RewriteAlgorithm scope) {
    this.relOptMaterialization = relOptMaterialization;
    this.scope = scope;
  }

  public RelOptMaterialization getRelOptMaterialization() {
    return relOptMaterialization;
  }

  public RewriteAlgorithm getScope() {
    return scope;
  }
}
