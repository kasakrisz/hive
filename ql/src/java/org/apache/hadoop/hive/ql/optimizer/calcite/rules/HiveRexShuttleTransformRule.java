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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.ivy.core.deliver.DeliverEngineSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class HiveRexShuttleTransformRule extends RelRule<HiveRexShuttleTransformRule.Config> {
  private HiveRexShuttleTransformRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    RelNode startNode = call.rel(0);
    RelNode rewriteNode = startNode.accept(config.shuttleFactory.apply(startNode.getCluster().getRexBuilder()));

    if (startNode == rewriteNode) {
      return;
    }

    RexBuilder rexBuilder = call.builder().getRexBuilder();
    List<RexNode> rootFieldList = new ArrayList<>(startNode.getRowType().getFieldCount());
    List<String> newColumnNames = new ArrayList<>(startNode.getRowType().getFieldCount());
    boolean nullabilityCastNeeded = false;
    for (int i = 0; i < startNode.getRowType().getFieldCount(); ++i) {
      RelDataTypeField startColType = startNode.getRowType().getFieldList().get(i);
      RelDataTypeField rewriteColType = rewriteNode.getRowType().getFieldList().get(i);
      RexNode rewriteInputRef = rexBuilder.makeInputRef(rewriteColType.getType(), i);

      if (startColType.getType().equals(rewriteColType.getType())) {
        rootFieldList.add(rewriteInputRef);
      } else {
        rootFieldList.add(rexBuilder.makeCast(startColType.getType(), rewriteInputRef));
        nullabilityCastNeeded = true;
      }

      newColumnNames.add(rewriteColType.getName());
    }

    if (!nullabilityCastNeeded) {
      call.transformTo(rewriteNode);
    }

    RelNode rewriteNodeWithCast = HiveRelFactories.HIVE_PROJECT_FACTORY.
            createProject(rewriteNode, ImmutableList.of(), rootFieldList, newColumnNames, ImmutableSet.of());

    call.transformTo(rewriteNodeWithCast);
  }

  public static class Config extends HiveRuleConfig {
    private Function<RexBuilder, RexShuttle> shuttleFactory;

    public Config withRexShuttle(Function<RexBuilder, RexShuttle> shuttleFactory) {
      this.shuttleFactory = Objects.requireNonNull(shuttleFactory);
      return this;
    }

    @Override
    public HiveRexShuttleTransformRule toRule() {
      return new HiveRexShuttleTransformRule(this);
    }
  }

}
