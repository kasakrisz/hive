/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

public class HiveRemoveEmptySingleRules {

  public static final RelOptRule PROJECT_INSTANCE =
          PruneEmptyRules.RemoveEmptySingleRule.Config.EMPTY
                  .withDescription("HivePruneEmptyProject")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(Project.class, project -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule FILTER_INSTANCE =
          PruneEmptyRules.RemoveEmptySingleRule.Config.EMPTY
                  .withDescription("HivePruneEmptyFilter")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(Filter.class, singleRel -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule JOIN_LEFT_INSTANCE =
          PruneEmptyRules.JoinLeftEmptyRuleConfig.EMPTY
                  .withOperandSupplier(b0 ->
                          b0.operand(Join.class).inputs(
                                  b1 -> b1.operand(Values.class)
                                          .predicate(Values::isEmpty).noInputs(),
                                  b2 -> b2.operand(RelNode.class).anyInputs()))
                  .withDescription("HivePruneEmptyJoin(left)")
                  .as(PruneEmptyRules.JoinLeftEmptyRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule JOIN_RIGHT_INSTANCE =
          PruneEmptyRules.JoinRightEmptyRuleConfig.EMPTY
                  .withOperandSupplier(b0 ->
                          b0.operand(Join.class).inputs(
                                  b1 -> b1.operand(RelNode.class).anyInputs(),
                                  b2 -> b2.operand(Values.class).predicate(Values::isEmpty)
                                          .noInputs()))
                  .withDescription("HivePruneEmptyJoin(right)")
                  .as(PruneEmptyRules.JoinRightEmptyRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();
}
