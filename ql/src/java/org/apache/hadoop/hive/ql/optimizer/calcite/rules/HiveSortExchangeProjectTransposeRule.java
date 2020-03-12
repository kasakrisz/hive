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

import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class HiveSortExchangeProjectTransposeRule extends RelOptRule {

  public static final HiveSortExchangeProjectTransposeRule INSTANCE =
          new HiveSortExchangeProjectTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveSortProjectTransposeRule.
   */
  private HiveSortExchangeProjectTransposeRule() {
    super(
            operand(
                    HiveSortExchange.class,
                    operand(HiveProject.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final SortExchange sortExchange = call.rel(0);
    final Project project = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    if (sortExchange.getConvention() != project.getConvention()) {
      return;
    }

    // Determine mapping between project input and output fields. If sortExchange
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
            RelOptUtil.permutationIgnoreCast(
                    project.getProjects(), project.getInput().getRowType());
    for (RelFieldCollation fc : sortExchange.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
      final RexNode node = project.getProjects().get(fc.getFieldIndex());
      if (node.isA(SqlKind.CAST)) {
        // Check whether it is a monotonic preserving cast, otherwise we cannot push
        final RexCall cast = (RexCall) node;
        final RexCallBinding binding =
                RexCallBinding.create(cluster.getTypeFactory(), cast,
                        ImmutableList.of(RelCollations.of(RexUtil.apply(map, fc))));
        if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
          return;
        }
      }
    }
    final RelCollation newCollation =
            cluster.traitSet().canonize(
                    RexUtil.apply(map, sortExchange.getCollation()));
    final SortExchange newSort =
            sortExchange.copy(
                    sortExchange.getTraitSet().replace(newCollation),
                    project.getInput(),
                    sortExchange.getDistribution(),
                    newCollation);
    RelNode newProject =
            project.copy(
                    sortExchange.getTraitSet(),
                    ImmutableList.of(newSort));
    // Not only is newProject equivalent to sortExchange;
    // newSort is equivalent to project's input
    // (but only if the sortExchange is not also applying an offset/limit).
    Map<RelNode, RelNode> equiv;
//    if (sortExchange.offset == null
//            && sortExchange.fetch == null
//            && cluster.getPlanner().getRelTraitDefs()
//            .contains(RelCollationTraitDef.INSTANCE)) {
//      equiv = ImmutableMap.of((RelNode) newSort, project.getInput());
//    } else {
      equiv = ImmutableMap.of();
//    }
    call.transformTo(newProject, equiv);
  }
}
