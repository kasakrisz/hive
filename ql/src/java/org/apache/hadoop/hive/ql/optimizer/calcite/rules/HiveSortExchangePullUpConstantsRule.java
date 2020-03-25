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
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner rule that pulls up constant keys through a SortExchange operator.
 */
public final class HiveSortExchangePullUpConstantsRule extends HiveSortPullUpConstantsRuleBase<HiveSortExchange> {
  protected static final Logger LOG = LoggerFactory.getLogger(HiveSortLimitPullUpConstantsRule.class);

  public static final HiveSortExchangePullUpConstantsRule INSTANCE =
          new HiveSortExchangePullUpConstantsRule();

  private HiveSortExchangePullUpConstantsRule() {
    super(HiveSortExchange.class, HiveRelFactories.HIVE_BUILDER);
  }

  @Override
  protected RelCollation getRelCollation(HiveSortExchange sortNode) {
    return sortNode.getCollation();
  }

  @Override
  protected void buildSort(RelBuilder relBuilder, HiveSortExchange sortNode, List<RelFieldCollation> fieldCollations) {
    relBuilder.sortExchange(
            HiveRelDistribution.from(fieldCollations, sortNode.getDistribution().getType()),
            RelCollations.of(fieldCollations));
  }
}
