package org.apache.hadoop.hive.ql.parse;

import org.apache.calcite.rel.RelNode;

public class CBOPlan {
  private final RelNode plan;
  private final String invalidAutomaticRewritingMaterializationReason;

  public CBOPlan(RelNode plan, String invalidAutomaticRewritingMaterializationReason) {
    this.plan = plan;
    this.invalidAutomaticRewritingMaterializationReason = invalidAutomaticRewritingMaterializationReason;
  }

  public RelNode getPlan() {
    return plan;
  }

  public String getInvalidAutomaticRewritingMaterializationReason() {
    return invalidAutomaticRewritingMaterializationReason;
  }
}
