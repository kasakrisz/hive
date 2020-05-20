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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCardinalityPreservingJoinRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCardinalityPreservingJoinRule.class);

  private static final HepProgram PROGRAM = new HepProgramBuilder()
      .addRuleInstance(HiveHepExtractRelNodeRule.INSTANCE)
      .build();

  private boolean triggered;

  public HiveCardinalityPreservingJoinRule() {
    super(operand(RelNode.class, any()),
        HiveRelFactories.HIVE_BUILDER, "HiveCardinalityPreservingJoinRule");
    triggered = false;
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
    final HepPlanner tmpPlanner = new HepPlanner(PROGRAM);
    tmpPlanner.setRoot(node);
    node = tmpPlanner.findBestExp();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Plan before:\n" + RelOptUtil.toString(node));
    }
    RelNode optimized = new HiveCardinalityPreservingJoinOptimization().trim(call.builder(), node);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Plan after:\n" + RelOptUtil.toString(optimized));
    }
    call.transformTo(optimized);

    triggered = true;
  }

  private static class HiveHepExtractRelNodeRule extends RelOptRule {

    private static final HiveHepExtractRelNodeRule INSTANCE =
        new HiveHepExtractRelNodeRule();

    private HiveHepExtractRelNodeRule() {
      super(operand(HepRelVertex.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HepRelVertex rel = call.rel(0);
      call.transformTo(rel.getCurrentRel());
    }
  }
}
