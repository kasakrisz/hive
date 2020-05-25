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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Cardinality preserving join optimization.
 * This optimization targets queries with one or more tables joined multiple times on their keys
 * and several columns are projected from those tables.
 *
 * Example:
 * with sq as (
 * select c_customer_id customer_id
 *       ,c_first_name customer_first_name
 *       ,c_last_name customer_last_name
 *   from customer
 * )
 * select c1.customer_id
 *       ,c1.customer_first_name
 *       ,c1.customer_last_name
 *  from sq c1
 *      ,sq c2
 *       ...
 *      ,sq cn
 * where c1.customer_id = c2.customer_id
 *   and ...
 *
 * In this case all column data in the cte will be shuffled.
 *
 * Goal of this optimization: rewrite the plan to include only primary key or non null unique key columns of
 * affected tables and join the them back to the result set of the main query to fetch the rest of the wide columns.
 * This reduces the data size of the affected tables that is broadcast/shuffled throughout the DAG processing.
 */
public class HiveCardinalityPreservingJoinOptimization extends HiveRelFieldTrimmer {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCardinalityPreservingJoinOptimization.class);
  private static final ThreadLocal<Map<RelOptHiveTable, SourceTable>> PROJECT_SOURCE_TABLES = new ThreadLocal<>();

  public HiveCardinalityPreservingJoinOptimization() {
    super(false);
  }

  @Override
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    try {
      REL_BUILDER.set(relBuilder);

      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      RelNode rootInput = root.getInput(0);
      List<RexInputRef> rootFieldList = new ArrayList<>(rootInput.getRowType().getFieldCount());
      for (int i = 0; i < rootInput.getRowType().getFieldList().size(); ++i) {
        RelDataTypeField relDataTypeField = rootInput.getRowType().getFieldList().get(i);
        rootFieldList.add(rexBuilder.makeInputRef(relDataTypeField.getType(), i));
      }

      ImmutableBitSet fieldsUsed = ImmutableBitSet.of();
      PROJECT_SOURCE_TABLES.set(new HashMap<>());

      Map<RelOptHiveTable, ProjectedFields> lineageMap = getExpressionLineageOf(rootFieldList, rootInput);

      if (lineageMap == null) {
        LOG.debug("Some project lineage can not be determined");
        return root;
      }

      for (Map.Entry<RelOptHiveTable, ProjectedFields> entry : lineageMap.entrySet()) {
        RelOptHiveTable table = entry.getKey();
        ProjectedFields projectedFields = entry.getValue();
        Optional<ImmutableBitSet> projectedKeys = table.getNonNullableKeys().stream()
            .filter(keys -> projectedFields.fieldsInSourceTable.contains(keys))
            .findFirst();

        if (projectedKeys.isPresent()) {
          SourceTable sourceTable = new SourceTable(projectedKeys.get(), projectedFields);
          PROJECT_SOURCE_TABLES.get().put(table, sourceTable);
          fieldsUsed = fieldsUsed.union(projectedFields.getSource(projectedKeys.get()));
        } else {
          fieldsUsed = fieldsUsed.union(projectedFields.fieldsInRootProject);
        }
      }

      if (PROJECT_SOURCE_TABLES.get().isEmpty()) {
        LOG.debug("None of the tables has keys projected, unable to join back");
        return root;
      }

      Set<RelDataTypeField> extraFields = Collections.emptySet();
      TrimResult trimResult = dispatchTrimFields(rootInput, fieldsUsed, extraFields);

      if (PROJECT_SOURCE_TABLES.get().values().stream().anyMatch(sourceTable -> sourceTable.hiveTableScan == null)) {
        LOG.debug("Unable to find HiveTableScan operator of some tables");
        return root;
      }

      RelNode newInput = trimResult.left;
      List<RexNode> newProjects = new ArrayList<>(rootFieldList.size());
      List<String> newColumnNames = new ArrayList<>(rootFieldList.size());
      for (int newProjectIndex = 0; newProjectIndex < fieldsUsed.cardinality(); ++newProjectIndex) {
        RelDataTypeField relDataTypeField = newInput.getRowType().getFieldList().get(newProjectIndex);
        newProjects.add(rexBuilder.makeInputRef(
            relDataTypeField.getType(), newProjectIndex));
        newColumnNames.add(relDataTypeField.getName());
      }

      // Create joins
      for (SourceTable sourceTable : PROJECT_SOURCE_TABLES.get().values()) {
        HiveTableScan originalTableScan = sourceTable.hiveTableScan;
        HiveTableScan tableScan = originalTableScan.copy(originalTableScan.getRowType());
        RelNode projectTableAccessRel = tableScan.project(
            sourceTable.projectedFields.fieldsInSourceTable, new HashSet<>(0), REL_BUILDER.get());

        Mapping keyMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
            tableScan.getRowType().getFieldCount(), sourceTable.keys.cardinality());
        int projectSourceIndex = 0;
        int offset = newProjects.size();
        for (int source : sourceTable.projectedFields.fieldsInSourceTable) {
          if (sourceTable.keys.get(source)) {
            keyMapping.set(source, projectSourceIndex);
          } else {
            RelDataTypeField relDataTypeField =
                projectTableAccessRel.getRowType().getFieldList().get(projectSourceIndex);
            newProjects.add(relBuilder.getRexBuilder().makeInputRef(
                relDataTypeField.getType(),
                offset + projectSourceIndex));
            newColumnNames.add(relDataTypeField.getName());
          }
          ++projectSourceIndex;
        }

        relBuilder.push(newInput);
        relBuilder.push(projectTableAccessRel);

        RexNode joinCondition = joinCondition(
            newInput, sourceTable, projectTableAccessRel, keyMapping, relBuilder.getRexBuilder());

        newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
      }

      relBuilder.push(newInput);
      relBuilder.project(newProjects, newColumnNames);

      root.replaceInput(0, relBuilder.build());
      return root;
    } finally {
      REL_BUILDER.remove();
      PROJECT_SOURCE_TABLES.remove();
    }
  }

  private Map<RelOptHiveTable, ProjectedFields> getExpressionLineageOf(
      List<RexInputRef> projectExpressions, RelNode projectInput) {
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    Map<RelOptHiveTable, ProjectedFields> rexTableInputRefList = new HashMap<>();
    for (RexInputRef expr : projectExpressions) {
      Set<RexNode> expressionLineage = relMetadataQuery.getExpressionLineage(projectInput, expr);
      if (expressionLineage == null || expressionLineage.size() != 1) {
        LOG.debug("Lineage can not be determined of expression: " + expr);
        return null;
      }

      RexTableInputRef rexTableInputRef = rexTableInputRef(expressionLineage.iterator().next());
      if (rexTableInputRef == null) {
        return null;
      }

      RelOptHiveTable relOptHiveTable = (RelOptHiveTable) rexTableInputRef.getTableRef().getTable();
      ProjectedFields projectedFields = rexTableInputRefList.computeIfAbsent(
          relOptHiveTable, k -> new ProjectedFields());
      projectedFields.fieldsInRootProject = projectedFields.fieldsInRootProject.set(expr.getIndex());
      projectedFields.fieldsInSourceTable = projectedFields.fieldsInSourceTable.set(rexTableInputRef.getIndex());
      projectedFields.mapping.add(new ProjectMapping(expr.getIndex(), rexTableInputRef.getIndex()));
    }

    return rexTableInputRefList;
  }

  public RexTableInputRef rexTableInputRef(RexNode rexNode) {
    if (rexNode.getKind() == SqlKind.TABLE_INPUT_REF) {
      return (RexTableInputRef) rexNode;
    }
    if (rexNode.getKind() == SqlKind.CAST) {
      RexCall rexCall = (RexCall) rexNode;
      return rexTableInputRef(rexCall.getOperands().get(0));
    }
    LOG.debug("Unable determine expression lineage " + rexNode);
    return null;
  }

  private RexNode joinCondition(
      RelNode newInput,
      SourceTable sourceTable, RelNode sourceProject, Mapping sourceKeyMapping,
      RexBuilder rexBuilder) {

    List<RexNode> equalsConditions = new ArrayList<>(sourceTable.keys.size());
    for (ProjectMapping projectMapping : sourceTable.projectedFields.mapping) {
      if (!sourceTable.keys.get(projectMapping.indexInSourceTable)) {
        continue;
      }

      int leftKeyIndex = projectMapping.indexInRootProject;
      RelDataTypeField leftKeyField = newInput.getRowType().getFieldList().get(leftKeyIndex);
      int rightKeyIndex = sourceKeyMapping.getTarget(projectMapping.indexInSourceTable);
      RelDataTypeField rightKeyField = sourceProject.getRowType().getFieldList().get(rightKeyIndex);

      equalsConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
          rexBuilder.makeInputRef(rightKeyField.getValue(),
              newInput.getRowType().getFieldCount() + rightKeyIndex)));
    }
    return RexUtil.composeConjunction(rexBuilder, equalsConditions);
  }

  private static final class ProjectMapping {
    private final int indexInRootProject;
    private final int indexInSourceTable;

    private ProjectMapping(int indexInRootProject, int indexInSourceTable) {
      this.indexInRootProject = indexInRootProject;
      this.indexInSourceTable = indexInSourceTable;
    }
  }

  private static final class ProjectedFields {
    private ImmutableBitSet fieldsInRootProject = ImmutableBitSet.of();
    private ImmutableBitSet fieldsInSourceTable = ImmutableBitSet.of();
    private final List<ProjectMapping> mapping = new ArrayList<>();

    public ImmutableBitSet getSource(ImmutableBitSet fields) {
      ImmutableBitSet targetFields = ImmutableBitSet.of();
      for (ProjectMapping fieldMapping : mapping) {
        if (fields.get(fieldMapping.indexInSourceTable)) {
          targetFields = targetFields.set(fieldMapping.indexInRootProject);
        }
      }
      return targetFields;
    }
  }

  private static final class SourceTable {
    private final ProjectedFields projectedFields;
    private final ImmutableBitSet keys;
    private HiveTableScan hiveTableScan;

    private SourceTable(ImmutableBitSet keys, ProjectedFields projectedFields) {
      this.projectedFields = projectedFields;
      this.keys = keys;
    }
  }

  @Override
  public TrimResult trimFields(
      HiveTableScan tableAccessRel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    TrimResult result = super.trimFields(tableAccessRel, fieldsUsed, extraFields);
    RelOptHiveTable table = (RelOptHiveTable) tableAccessRel.getTable();
    SourceTable sourceTable = PROJECT_SOURCE_TABLES.get().get(table);
    if (sourceTable != null) {
      sourceTable.hiveTableScan = tableAccessRel;
    }
    return result;
  }
}

