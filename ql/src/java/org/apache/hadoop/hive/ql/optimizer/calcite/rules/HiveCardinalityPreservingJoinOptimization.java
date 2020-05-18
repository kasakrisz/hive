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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HiveCardinalityPreservingJoinOptimization extends HiveRelFieldTrimmer {

  private ThreadLocal<Map<RelOptHiveTable, SourceTable>> projectSourceTables;

  public HiveCardinalityPreservingJoinOptimization() {
    super(false);
  }

  @Override
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    try {
      REL_BUILDER.set(relBuilder);

      HiveProject rootProject;
      RelNode previousRelNode = null;
      RelNode relNode = root;
      while (!(relNode instanceof HiveProject)) {
        if (relNode.getInputs().size() != 1) {
          return root;
        }
        previousRelNode = relNode;
        relNode = relNode.getInput(0);
      }

      rootProject = (HiveProject) relNode;

      RelNode rootProjectInput = rootProject.getInput(0);
      ImmutableBitSet fieldsUsed = ImmutableBitSet.of();
      projectSourceTables = ThreadLocal.withInitial(HashMap::new);

      Map<RelOptHiveTable, ProjectedFields> rexTableInputRefList =
          getExpressionLineageOf(rootProject.getProjects(), rootProjectInput);

      if (rexTableInputRefList == null) {
        // Some project lineage can not be determined
        return root;
      }

      for (RelOptHiveTable relOptHiveTable : rexTableInputRefList.keySet()) {
        ProjectedFields projectedFields = rexTableInputRefList.get(relOptHiveTable);
        Optional<ImmutableBitSet> projectedKeys = relOptHiveTable.getNonNullableKeys().stream()
            .filter(keys -> projectedFields.fieldsInSourceTable.contains(keys))
            .findFirst();

        if (projectedKeys.isPresent()) {
          SourceTable sourceTable = new SourceTable(projectedKeys.get(), projectedFields);
          projectSourceTables.get().put(relOptHiveTable, sourceTable);
          fieldsUsed = fieldsUsed.union(projectedFields.getSource(projectedKeys.get()));
        } else {
          fieldsUsed = fieldsUsed.union(projectedFields.fieldsInRootProject);
        }
      }

      if (projectSourceTables.get().isEmpty()) {
        // None of the tables has keys projected, unable to join back
        return root;
      }

      Set<RelDataTypeField> extraFields = Collections.emptySet();
      final TrimResult trimResult = dispatchTrimFields(rootProjectInput, fieldsUsed, extraFields);

      if (projectSourceTables.get().values().stream().anyMatch(sourceTable -> sourceTable.hiveTableScan == null)) {
        return root;
      }

      final RexBuilder rexBuilder = REL_BUILDER.get().getRexBuilder();

      RelNode newInput = trimResult.left;
      Map<RelOptHiveTable, Pair<Integer, Mapping>> offsetMap = new HashMap<>();
      for (Map.Entry<RelOptHiveTable, SourceTable> sourceTableEntry : projectSourceTables.get().entrySet()) {
        RelOptHiveTable relOptHiveTable = sourceTableEntry.getKey();
        SourceTable sourceTable = sourceTableEntry.getValue();

        // Create mapping for projected fields from current table
        final Mapping rightProjectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
            relOptHiveTable.getRowType().getFieldCount(), sourceTable.projectedFields.fieldsInSourceTable.cardinality());
        int idx = 0;
        for (Integer bit : sourceTable.projectedFields.fieldsInSourceTable) {
          if (sourceTable.projectedFields.fieldsInSourceTable.get(bit)) {
            rightProjectMapping.set(bit, idx);
          }
          ++idx;
        }
        offsetMap.put(relOptHiveTable, new Pair<>(newInput.getRowType().getFieldCount(), rightProjectMapping));

        // New TableScan and Project for current table
        HiveTableScan originalTableScan = sourceTableEntry.getValue().hiveTableScan;
        HiveTableScan tableScan = originalTableScan.copy(originalTableScan.getRowType());
        RelNode projectTableAccessRel = tableScan.project(sourceTable.projectedFields.fieldsInSourceTable, new HashSet<>(0), REL_BUILDER.get());

        relBuilder.push(newInput);
        relBuilder.push(projectTableAccessRel);

        RexNode joinCondition = null;
        boolean first = true;
        for (ProjectMapping projectMapping : sourceTable.projectedFields.mapping) {
          if (!sourceTable.keys.get(projectMapping.indexInSourceTable)) {
            continue;
          }

          int leftKeyIndex = projectMapping.indexInRootProject;
          RelDataTypeField leftKeyField = newInput.getRowType().getFieldList().get(leftKeyIndex);
          int rightKeyIndex = projectMapping.indexInSourceTable;
          RelDataTypeField rightKeyField = tableScan.getRowType().getFieldList().get(rightKeyIndex);

          RexNode equalsCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
              rexBuilder.makeInputRef(rightKeyField.getValue(),
                  newInput.getRowType().getFieldCount() + rightKeyIndex));

          if (first) {
            joinCondition = equalsCondition;
            first = false;
          } else {
            joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, joinCondition, equalsCondition);
          }
        }

        newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
      }

      final Mapping rootProjectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
          newInput.getRowType().getFieldCount(), newInput.getRowType().getFieldCount());
      for (RelDataTypeField field : trimResult.left.getRowType().getFieldList()) {
        if (fieldsUsed.get(field.getIndex())) {
          rootProjectMapping.set(field.getIndex(), trimResult.right.getTarget(field.getIndex()));
        }
      }

      for (Map.Entry<RelOptHiveTable, SourceTable> entry : projectSourceTables.get().entrySet()) {
        RelOptHiveTable relOptHiveTable = entry.getKey();
        for (ProjectMapping projectMapping : entry.getValue().projectedFields.mapping) {
          if (!entry.getValue().keys.get(projectMapping.indexInSourceTable)) {
            Pair<Integer, Mapping> offsetEntry = offsetMap.get(relOptHiveTable);
            int targetFieldIdx = offsetEntry.left + offsetEntry.right.getTarget(projectMapping.indexInSourceTable);
            rootProjectMapping.set(projectMapping.indexInRootProject, targetFieldIdx);
          }
        }
      }

      // Build new project expressions.
      final List<RexNode> newProjects = new ArrayList<>();
      final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(rootProjectMapping, newInput);
      for (Ord<RexNode> ord : Ord.zip(rootProject.getProjects())) {
        RexNode newProjectExpr = ord.e.accept(shuttle);
        newProjects.add(newProjectExpr);
      }

      relBuilder.push(newInput);
      relBuilder.project(newProjects, rootProject.getRowType().getFieldNames());

      RelNode newRootProject = relBuilder.build();
      if (previousRelNode == null) {
        return newRootProject;
      }

      previousRelNode.replaceInput(0, newRootProject);
      return root;
    }
    finally {
      REL_BUILDER.remove();
      if (projectSourceTables != null) {
        projectSourceTables.remove();
      }
    }
  }

  private Map<RelOptHiveTable, ProjectedFields> getExpressionLineageOf(
      List<RexNode> projectExpressions, RelNode projectInput) {
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    Map<RelOptHiveTable, ProjectedFields> rexTableInputRefList = new HashMap<>();
    for (RexNode expr : projectExpressions) {
      if (expr.getKind() != SqlKind.INPUT_REF) {
        return null;
      }

      RexSlot projectExpr = (RexSlot) expr;
      Set<RexNode> expressionLineage = relMetadataQuery.getExpressionLineage(projectInput, projectExpr);
      if (expressionLineage.size() != 1) {
        return null;
      }
      RexNode rexNode = expressionLineage.iterator().next();
      if (rexNode.getKind() != SqlKind.TABLE_INPUT_REF) {
        return null;
      }

      RexTableInputRef rexTableInputRef = (RexTableInputRef) rexNode;
      RelOptHiveTable relOptHiveTable = (RelOptHiveTable) rexTableInputRef.getTableRef().getTable();
      ProjectedFields projectedFields = rexTableInputRefList.computeIfAbsent(
          relOptHiveTable, k -> new ProjectedFields());
      projectedFields.fieldsInRootProject = projectedFields.fieldsInRootProject.set(projectExpr.getIndex());
      projectedFields.fieldsInSourceTable = projectedFields.fieldsInSourceTable.set(rexTableInputRef.getIndex());
      projectedFields.mapping.add(new ProjectMapping(projectExpr.getIndex(), rexTableInputRef.getIndex()));
    }

    return rexTableInputRefList;
  }

  private static class ProjectMapping {
    private final int indexInRootProject;
    private final int indexInSourceTable;

    private ProjectMapping(int indexInRootProject, int indexInSourceTable) {
      this.indexInRootProject = indexInRootProject;
      this.indexInSourceTable = indexInSourceTable;
    }
  }

  private static class ProjectedFields {
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

  private static class SourceTable {
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
    SourceTable sourceTable = projectSourceTables.get().get(table);
    if (sourceTable != null) {
      sourceTable.hiveTableScan = tableAccessRel;
    }
    return result;
  }
}

