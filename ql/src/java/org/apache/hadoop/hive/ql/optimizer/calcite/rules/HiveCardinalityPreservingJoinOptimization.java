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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCardinalityPreservingJoinOptimization extends HiveRelFieldTrimmer {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCardinalityPreservingJoinOptimization.class);
  private static final ThreadLocal<Map<RelOptHiveTable, SourceTable>> projectSourceTables = new ThreadLocal<>();

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
          LOG.debug("Current node has more than one input.");
          return null;
        }
        previousRelNode = relNode;
        relNode = relNode.getInput(0);
      }

      rootProject = (HiveProject) relNode;

      RelNode rootProjectInput = rootProject.getInput(0);
      ImmutableBitSet fieldsUsed = ImmutableBitSet.of();
      projectSourceTables.set(new HashMap<>());

      Map<RelOptHiveTable, ProjectedFields> lineageMap =
          getExpressionLineageOf(rootProject.getProjects(), rootProjectInput);

      if (lineageMap == null) {
        LOG.debug("Some project lineage can not be determined");
        return null;
      }

      for (Map.Entry<RelOptHiveTable, ProjectedFields> entry : lineageMap.entrySet()) {
        RelOptHiveTable table = entry.getKey();
        ProjectedFields projectedFields = entry.getValue();
        Optional<ImmutableBitSet> projectedKeys = table.getNonNullableKeys().stream()
            .filter(keys -> projectedFields.fieldsInSourceTable.contains(keys))
            .findFirst();

        if (projectedKeys.isPresent()) {
          SourceTable sourceTable = new SourceTable(projectedKeys.get(), projectedFields);
          projectSourceTables.get().put(table, sourceTable);
          fieldsUsed = fieldsUsed.union(projectedFields.getSource(projectedKeys.get()));
        } else {
          fieldsUsed = fieldsUsed.union(projectedFields.fieldsInRootProject);
        }
      }

      if (projectSourceTables.get().isEmpty()) {
        LOG.debug("None of the tables has keys projected, unable to join back");
        return null;
      }

      Set<RelDataTypeField> extraFields = Collections.emptySet();
      TrimResult trimResult = dispatchTrimFields(rootProjectInput, fieldsUsed, extraFields);

      if (projectSourceTables.get().values().stream().anyMatch(sourceTable -> sourceTable.hiveTableScan == null)) {
        LOG.debug("Unable to find HiveTableScan operator of some tables");
        return null;
      }

      // Create joins
      RelNode newInput = trimResult.left;
      Map<RelOptHiveTable, Pair<Integer, Mapping>> offsetMap = new HashMap<>();
      for (Map.Entry<RelOptHiveTable, SourceTable> sourceTableEntry : projectSourceTables.get().entrySet()) {
        RelOptHiveTable relOptHiveTable = sourceTableEntry.getKey();
        SourceTable sourceTable = sourceTableEntry.getValue();

        offsetMap.put(relOptHiveTable,
            new Pair<>(newInput.getRowType().getFieldCount(), sourceTable.projectedFields.createMapping()));

        newInput = joinBack(newInput, sourceTable, relBuilder);
      }

      Mapping rootProjectMapping = rootProjectMapping(fieldsUsed, trimResult, newInput, offsetMap);

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
      projectSourceTables.remove();
    }
  }

  private Map<RelOptHiveTable, ProjectedFields> getExpressionLineageOf(
      List<RexNode> projectExpressions, RelNode projectInput) {
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    Map<RelOptHiveTable, ProjectedFields> rexTableInputRefList = new HashMap<>();
    for (RexNode expr : projectExpressions) {
      if (expr.getKind() != SqlKind.INPUT_REF) {
        LOG.debug("Expression is not INPUT_REF: " + expr);
        return null;
      }

      RexSlot projectExpr = (RexSlot) expr;
      Set<RexNode> expressionLineage = relMetadataQuery.getExpressionLineage(projectInput, projectExpr);
      if (expressionLineage == null || expressionLineage.size() != 1) {
        LOG.debug("Lineage can not be determined of expression: " + expr);
        return null;
      }
      RexNode rexNode = expressionLineage.iterator().next();
      if (rexNode.getKind() != SqlKind.TABLE_INPUT_REF) {
        LOG.debug("Expression lineage is TABLE_INPUT_REF: " + expr + " but " + rexNode);
        return null;
      }

      RexTableInputRef rexTableInputRef = (RexTableInputRef) rexNode;
      RelOptHiveTable relOptHiveTable = (RelOptHiveTable) rexTableInputRef.getTableRef().getTable();
      ProjectedFields projectedFields = rexTableInputRefList.computeIfAbsent(
          relOptHiveTable, k -> new ProjectedFields(relOptHiveTable));
      projectedFields.fieldsInRootProject = projectedFields.fieldsInRootProject.set(projectExpr.getIndex());
      projectedFields.fieldsInSourceTable = projectedFields.fieldsInSourceTable.set(rexTableInputRef.getIndex());
      projectedFields.mapping.add(new ProjectMapping(projectExpr.getIndex(), rexTableInputRef.getIndex()));
    }

    return rexTableInputRefList;
  }

  private RelNode joinBack(RelNode newInput, SourceTable sourceTable, RelBuilder relBuilder) {
    HiveTableScan originalTableScan = sourceTable.hiveTableScan;
    HiveTableScan tableScan = originalTableScan.copy(originalTableScan.getRowType());
    RelNode projectTableAccessRel = tableScan.project(
        sourceTable.projectedFields.fieldsInSourceTable, new HashSet<>(0), REL_BUILDER.get());

    relBuilder.push(newInput);
    relBuilder.push(projectTableAccessRel);

    RexNode joinCondition = joinCondition(newInput, sourceTable, relBuilder.getRexBuilder(), tableScan);

    newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
    return newInput;
  }

  private RexNode joinCondition(
      RelNode newInput, SourceTable sourceTable, RexBuilder rexBuilder, HiveTableScan tableScan) {

    List<RexNode> equalsConditions = new ArrayList<>(sourceTable.keys.size());
    for (ProjectMapping projectMapping : sourceTable.projectedFields.mapping) {
      if (!sourceTable.keys.get(projectMapping.indexInSourceTable)) {
        continue;
      }

      int leftKeyIndex = projectMapping.indexInRootProject;
      RelDataTypeField leftKeyField = newInput.getRowType().getFieldList().get(leftKeyIndex);
      int rightKeyIndex = projectMapping.indexInSourceTable;
      RelDataTypeField rightKeyField = tableScan.getRowType().getFieldList().get(rightKeyIndex);

      equalsConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
          rexBuilder.makeInputRef(rightKeyField.getValue(),
              newInput.getRowType().getFieldCount() + rightKeyIndex)));
    }
    return RexUtil.composeConjunction(rexBuilder, equalsConditions);
  }

  private Mapping rootProjectMapping(ImmutableBitSet fieldsUsed, TrimResult trimResult, RelNode newInput,
                                     Map<RelOptHiveTable, Pair<Integer, Mapping>> offsetMap) {
    final Mapping rootProjectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
        newInput.getRowType().getFieldCount(), newInput.getRowType().getFieldCount());
    addFieldsFromSubTree(fieldsUsed, trimResult, rootProjectMapping);
    addFieldsFromSourceTables(offsetMap, rootProjectMapping);
    return rootProjectMapping;
  }

  private void addFieldsFromSubTree(ImmutableBitSet fieldsUsed, TrimResult trimResult, Mapping rootProjectMapping) {
    for (RelDataTypeField field : trimResult.left.getRowType().getFieldList()) {
      if (fieldsUsed.get(field.getIndex())) {
        rootProjectMapping.set(field.getIndex(), trimResult.right.getTarget(field.getIndex()));
      }
    }
  }

  // collect fields from joined back tables minus keys (since they are coming from the other side)
  private void addFieldsFromSourceTables(Map<RelOptHiveTable, Pair<Integer, Mapping>> offsetMap,
                                         Mapping rootProjectMapping) {
    for (Map.Entry<RelOptHiveTable, SourceTable> entry : projectSourceTables.get().entrySet()) {
      RelOptHiveTable relOptHiveTable = entry.getKey();
      SourceTable sourceTable = entry.getValue();
      for (ProjectMapping projectMapping : sourceTable.projectedFields.mapping) {
        if (!sourceTable.keys.get(projectMapping.indexInSourceTable)) {
          Pair<Integer, Mapping> offsetEntry = offsetMap.get(relOptHiveTable);
          int targetFieldIdx = offsetEntry.left + offsetEntry.right.getTarget(projectMapping.indexInSourceTable);
          rootProjectMapping.set(projectMapping.indexInRootProject, targetFieldIdx);
        }
      }
    }
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
    private final RelOptHiveTable relOptHiveTable;
    private ImmutableBitSet fieldsInRootProject = ImmutableBitSet.of();
    private ImmutableBitSet fieldsInSourceTable = ImmutableBitSet.of();
    private final List<ProjectMapping> mapping = new ArrayList<>();

    private ProjectedFields(RelOptHiveTable relOptHiveTable) {
      this.relOptHiveTable = relOptHiveTable;
    }

    public ImmutableBitSet getSource(ImmutableBitSet fields) {
      ImmutableBitSet targetFields = ImmutableBitSet.of();
      for (ProjectMapping fieldMapping : mapping) {
        if (fields.get(fieldMapping.indexInSourceTable)) {
          targetFields = targetFields.set(fieldMapping.indexInRootProject);
        }
      }
      return targetFields;
    }

    public Mapping createMapping() {
      final Mapping rightProjectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
          relOptHiveTable.getRowType().getFieldCount(), fieldsInSourceTable.cardinality());
      int idx = 0;
      for (Integer bit : fieldsInSourceTable) {
        if (fieldsInSourceTable.get(bit)) {
          rightProjectMapping.set(bit, idx);
        }
        ++idx;
      }
      return rightProjectMapping;
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

