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

import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that pulls up constant keys through a SortLimit operator.
 * 
 * This rule is only applied on SortLimit operators that are not the root
 * of the plan tree. This is done because the interaction of this rule
 * with the AST conversion may cause some optimizations to not kick in
 * e.g. SimpleFetchOptimizer. Nevertheless, this will not have any
 * performance impact in the resulting plans.
 */
public final class HiveSortLimitPullUpConstantsRule extends HiveSortPullUpConstantsRuleBase<HiveSortLimit> {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveSortLimitPullUpConstantsRule.class);


  public static final HiveSortLimitPullUpConstantsRule INSTANCE =
          new HiveSortLimitPullUpConstantsRule();

  private HiveSortLimitPullUpConstantsRule() {
    super(HiveSortLimit.class, HiveRelFactories.HIVE_BUILDER);
  }

  @Override
  protected void buildSort(RelBuilder relBuilder, HiveSortLimit sortNode, Mappings.TargetMapping mapping) {
    List<RelFieldCollation> fieldCollations = applyToFieldCollations(sortNode.getCollation(), mapping);
    final ImmutableList<RexNode> sortFields =
            relBuilder.fields(RelCollations.of(fieldCollations));
    relBuilder.sortLimit(sortNode.offset == null ? -1 : RexLiteral.intValue(sortNode.offset),
            sortNode.fetch == null ? -1 : RexLiteral.intValue(sortNode.fetch), sortFields);
  }
}
