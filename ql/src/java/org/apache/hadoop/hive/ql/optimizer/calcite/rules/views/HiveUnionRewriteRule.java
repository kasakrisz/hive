/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveHepExtractRelNodeRule;


public class HiveUnionRewriteRule extends RelOptRule {

  private final HiveRelOptMaterialization materialization;
  private boolean triggered;

  public HiveUnionRewriteRule(HiveRelOptMaterialization materialization) {
    this( "HiveUnionRewriteRule", materialization);
  }

  protected HiveUnionRewriteRule(String description, HiveRelOptMaterialization materialization) {
    super(operand(RelNode.class, any()),
        HiveRelFactories.HIVE_BUILDER, description);
    this.materialization = materialization;
    this.triggered = false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (triggered) {
      // Bail out
      return;
    }

    RelNode node = call.rel(0);
    final HepRelVertex root = (HepRelVertex) call.getPlanner().getRoot();
    if (root.getCurrentRel() != node) {
      // Bail out
      return;
    }
    // The node is the root, release the kraken!
    node = HiveHepExtractRelNodeRule.execute(node);
    call.transformTo(rewrite(call, node));
    triggered = true;
  }

  protected RelNode rewrite(RelOptRuleCall call, RelNode basePlan) {
    final RelOptCluster optCluster = basePlan.getCluster();
    final RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(optCluster, null);

    return relBuilder
        .push(materialization.queryRel)
        .push(materialization.tableRel)
        .union(true)
        .build();
  }
}
