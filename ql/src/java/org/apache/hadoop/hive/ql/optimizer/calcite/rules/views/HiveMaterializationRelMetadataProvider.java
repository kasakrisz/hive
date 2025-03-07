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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.Set;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdAllPredicates;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveRelMdCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewBoxing.Box;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata provider implementation that is only used for MV rewriting.
 */
public class HiveMaterializationRelMetadataProvider {

  private static final RelMetadataProvider SOURCE_NODE_TYPES =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.NODE_TYPES.method, new HiveRelMdNodeTypes());

  private static final RelMetadataProvider SOURCE_EXPRESSION_LINEAGE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.EXPRESSION_LINEAGE.method, new HiveRelMdExpressionLineage());

  private static final RelMetadataProvider SOURCE_ALL_PREDICATES =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ALL_PREDICATES.method, new HiveRelMdAllPredicates());

  private static final RelMetadataProvider SOURCE_TABLE_REFERENCES =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.TABLE_REFERENCES.method, new HiveRelMdTableReferences());

  public static final RelMetadataProvider SOURCE_ROW_COUNT = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new HiveRelMdRowCount());

  public static final JaninoRelMetadataProvider DEFAULT =
      JaninoRelMetadataProvider.of(
          ChainedRelMetadataProvider.of(
              ImmutableList.of(
                  SOURCE_ROW_COUNT,
                  new HiveMaterializationRelMdCost().getMetadataProvider(),
                  SOURCE_NODE_TYPES,
                  SOURCE_EXPRESSION_LINEAGE,
                  SOURCE_ALL_PREDICATES,
                  SOURCE_TABLE_REFERENCES,
                  JaninoRelMetadataProvider.DEFAULT)));


  private static class HiveRelMdNodeTypes extends RelMdNodeTypes {
    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelSubset rel,
        RelMetadataQuery mq) {
      for (RelNode node : rel.getRelList()) {
        if (node instanceof Box) {
          return mq.getNodeTypes(node);
        }
      }
      return mq.getNodeTypes(Util.first(rel.getBest(), rel.getOriginal()));
    }
  }

  private static class HiveRelMdExpressionLineage extends RelMdExpressionLineage {
    public Set<RexNode> getExpressionLineage(RelSubset rel,
        RelMetadataQuery mq, RexNode outputExpression) {
      for (RelNode node : rel.getRelList()) {
        if (node instanceof Box) {
          return mq.getExpressionLineage(node, outputExpression);
        }
      }
      return mq.getExpressionLineage(Util.first(rel.getBest(), rel.getOriginal()),
          outputExpression);
    }
  }

  private static class HiveRelMdAllPredicates extends RelMdAllPredicates {
    public RelOptPredicateList getAllPredicates(RelSubset rel,
        RelMetadataQuery mq) {
      for (RelNode node : rel.getRelList()) {
        if (node instanceof Box) {
          return mq.getAllPredicates(node);
        }
      }
      return mq.getAllPredicates(Util.first(rel.getBest(), rel.getOriginal()));
    }
  }

  private static class HiveRelMdTableReferences extends RelMdTableReferences {
    public Set<RelTableRef> getTableReferences(RelSubset rel, RelMetadataQuery mq) {
      for (RelNode node : rel.getRelList()) {
        if (node instanceof Box) {
          return mq.getTableReferences(node);
        }
      }
      return mq.getTableReferences(Util.first(rel.getBest(), rel.getOriginal()));
    }
  }

  private static class HiveRelMdRowCount extends RelMdRowCount {
    private static final Logger LOG  = LoggerFactory.getLogger(HiveRelMdRowCount.class);

    @Override
    public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
      if ("mv_filter".equals(rel.getTable().getQualifiedName().get(1))) {
        LOG.info("Returning null for mv_filter row count ");
        return Double.POSITIVE_INFINITY;
      }
      try {
        LOG.info("Estimate row count of {}", rel.getTable().getQualifiedName().get(1));
        return rel.estimateRowCount(mq);
      }
      catch (Exception ex) {
        LOG.warn("Error while getting row count from rel " + rel, ex);
        return Double.POSITIVE_INFINITY;
      }
    }
  }

  private static class HiveMaterializationRelMdCost extends HiveRelMdCost {

    public HiveMaterializationRelMdCost() {
      super(HiveDefaultCostModel.getCostModel());
    }

    @Override
    public RelMetadataProvider getMetadataProvider() {
      return ChainedRelMetadataProvider.of(
          ImmutableList.of(
              ReflectiveRelMetadataProvider.reflectiveSource(this, BuiltInMethod.NON_CUMULATIVE_COST.method),
              HiveMaterializationRelMdPercentageOriginalRows.SOURCE));
    }
  }

  private static class HiveMaterializationRelMdPercentageOriginalRows
      extends RelMdPercentageOriginalRows {
    private static final Logger LOG  = LoggerFactory.getLogger(HiveMaterializationRelMdPercentageOriginalRows.class);
    private static final RelMdPercentageOriginalRows INSTANCE =
        new HiveMaterializationRelMdPercentageOriginalRows();

    public static final RelMetadataProvider SOURCE =
        ChainedRelMetadataProvider.of(
            ImmutableList.of(
                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method, INSTANCE),

                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.CUMULATIVE_COST.method, INSTANCE),

                ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE)));

    @Override
    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
      if ("mv_filter".equals(rel.getTable().getQualifiedName().get(1))) {
        LOG.info("Returning null for mv_filter row count ");
        return HiveCost.INFINITY;
      }
      try {
        return super.getNonCumulativeCost(rel, mq);
      }
      catch (Exception ex) {
        LOG.warn("Error while calculating non cumulative cost of " + rel, ex);
        return HiveCost.INFINITY;
      }
    }
  }
}
