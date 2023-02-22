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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;

import java.util.ArrayList;
import java.util.List;


public class HiveUnionAggregateRewriteRule extends RelOptRule {

  private final HiveRelOptMaterialization materialization;
  private boolean triggered;

  public HiveUnionAggregateRewriteRule(HiveRelOptMaterialization materialization) {
    this( "HiveUnionRewriteRule", materialization);
  }

  protected HiveUnionAggregateRewriteRule(String description, HiveRelOptMaterialization materialization) {
    super(operand(HiveAggregate.class, any()),
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

    call.transformTo(rewrite(call.rel(0)));
    triggered = true;
  }

  protected RelNode rewrite(RelNode basePlan) {
    final RelOptCluster optCluster = basePlan.getCluster();
    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(optCluster, null);

    relBuilder = relBuilder
        .push(materialization.queryRel)
        .push(materialization.tableRel)
        .union(true);

    List<RexNode> exprList = new ArrayList<>(relBuilder.peek().getRowType().getFieldCount());
    List<String> nameList = new ArrayList<>(relBuilder.peek().getRowType().getFieldCount());
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    for (int i = 0; i < relBuilder.peek().getRowType().getFieldCount(); i++) {
      // We can take unionInputQuery as it is query based.
      RelDataTypeField field = materialization.queryRel.getRowType().getFieldList().get(i);
      exprList.add(
              rexBuilder.ensureType(
                      field.getType(),
                      rexBuilder.makeInputRef(relBuilder.peek(), i),
                      true));
      nameList.add(field.getName());
    }
    relBuilder.project(exprList, nameList);
    // Rollup aggregate
    Aggregate aggregate = (Aggregate) materialization.queryRel;
    final ImmutableBitSet groupSet = ImmutableBitSet.range(aggregate.getGroupCount());
    final List<RelBuilder.AggCall> aggregateCalls = new ArrayList<>();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggCall = aggregate.getAggCallList().get(i);
      if (aggCall.isDistinct()) {
        // Cannot ROLLUP distinct
        return null;
      }
      SqlAggFunction rollupAgg =
              HiveRelBuilder.getRollup(aggCall.getAggregation());
      if (rollupAgg == null) {
        // Cannot rollup this aggregate, bail out
        return null;
      }
      final RexInputRef operand =
              rexBuilder.makeInputRef(relBuilder.peek(),
                      aggregate.getGroupCount() + i);
      aggregateCalls.add(
              relBuilder.aggregateCall(rollupAgg, operand)
                      .distinct(aggCall.isDistinct())
                      .approximate(aggCall.isApproximate())
                      .as(aggCall.name));
    }
    RelNode prevNode = relBuilder.peek();
    RelNode result = relBuilder
            .aggregate(relBuilder.groupKey(groupSet), aggregateCalls)
            .build();
    if (prevNode == result && groupSet.cardinality() != result.getRowType().getFieldCount()) {
      // Aggregate was not inserted but we need to prune columns
      result = relBuilder
              .push(result)
              .project(relBuilder.fields(groupSet))
              .build();
    }
    // Result
    return result;
  }
}
