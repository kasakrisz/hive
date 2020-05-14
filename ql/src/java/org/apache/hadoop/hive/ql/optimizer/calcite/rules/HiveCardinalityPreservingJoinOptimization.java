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
import java.util.Objects;
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
import org.apache.calcite.util.mapping.IntPair;
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
      ImmutableBitSet projectedFields = ImmutableBitSet.of();
      ImmutableBitSet keyFieldsUsed = ImmutableBitSet.of();
      projectSourceTables = ThreadLocal.withInitial(HashMap::new);
      for (RexNode expr : rootProject.getProjects()) {
        RexSlot projectExpr = (RexSlot) expr;
        projectedFields = projectedFields.set(projectExpr.getIndex());
        Set<RexNode> expressionLineage = RelMetadataQuery.instance().getExpressionLineage(rootProjectInput, projectExpr);
        if (expressionLineage.size() != 1) {
          // Bail out
          return root;
        }
        RexNode rexNode = expressionLineage.iterator().next();
        if (rexNode.getKind() != SqlKind.TABLE_INPUT_REF) {
          // Bail out
          return root;
        }

        RexTableInputRef rexTableInputRef = (RexTableInputRef) rexNode;
        RelOptHiveTable relOptHiveTable = (RelOptHiveTable) rexTableInputRef.getTableRef().getTable();
        List<ImmutableBitSet> nonNullableKeys = relOptHiveTable.getNonNullableKeys();
        if (nonNullableKeys.isEmpty()) {
          continue;
        }

        SourceTable sourceTable = projectSourceTables.get().computeIfAbsent(relOptHiveTable, k -> new SourceTable());
        int indexInSourceTable = rexTableInputRef.getIndex();
        IntPair fieldMapping = new IntPair(projectExpr.getIndex(), indexInSourceTable);
        sourceTable.projectedFieldsMapping.add(fieldMapping);

        for (ImmutableBitSet keyFields : nonNullableKeys) {
          if (keyFields.get(indexInSourceTable)) {
            List<IntPair> keyMapping = sourceTable.keyMappings.computeIfAbsent(keyFields, kf -> new ArrayList<>());
            keyMapping.add(fieldMapping);
            keyFieldsUsed = keyFieldsUsed.set(fieldMapping.source);
            break;
          }
        }
      }

      // remove tables if not all the fields are projected from any of its keys
      for (Map.Entry<RelOptHiveTable, SourceTable> entry : projectSourceTables.get().entrySet()) {
        if (entry.getValue().getKeyMapping() == null) {
          projectSourceTables.get().remove(entry.getKey());
        }
      }

      Set<RelDataTypeField> extraFields = Collections.emptySet();
      final TrimResult trimResult = dispatchTrimFields(rootProjectInput, keyFieldsUsed, extraFields);

      projectSourceTables.get().values().removeIf(Objects::isNull);
      if (projectSourceTables.get().isEmpty()) {
        return root;
      }

      final RexBuilder rexBuilder = REL_BUILDER.get().getRexBuilder();

      RelNode newInput = trimResult.left;
      Map<RelOptHiveTable, Pair<Integer, Mapping>> offsetMap = new HashMap<>();
      for (Map.Entry<RelOptHiveTable, SourceTable> sourceTableEntry : projectSourceTables.get().entrySet()) {
        RelOptHiveTable relOptHiveTable = sourceTableEntry.getKey();

        // Fields need to be projected from current table
        SourceTable sourceTable = sourceTableEntry.getValue();
        ImmutableBitSet fieldsProjected = ImmutableBitSet.of();
        for (IntPair fieldMapping : sourceTable.projectedFieldsMapping) {
          fieldsProjected = fieldsProjected.set(fieldMapping.target);
        }

        ImmutableBitSet keyFields = ImmutableBitSet.of();
        for (IntPair keyMapping : sourceTable.getKeyMapping()) {
          keyFields = keyFields.set(keyMapping.target);
        }

        // Projected and key fields for join from current table
        ImmutableBitSet fieldsUnion = fieldsProjected.union(keyFields);

        // Create mapping for projected fields from current table
        final Mapping rightProjectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
            relOptHiveTable.getRowType().getFieldCount(), fieldsUnion.cardinality());
        int idx = 0;
        for (Integer bit : fieldsUnion) {
          if (fieldsProjected.get(bit)) {
            rightProjectMapping.set(bit, idx);
          }
          ++idx;
        }
        offsetMap.put(relOptHiveTable, new Pair<>(newInput.getRowType().getFieldCount(), rightProjectMapping));

        // New TableScan and Project for current table
        HiveTableScan originalTableScan = sourceTableEntry.getValue().hiveTableScan;
        HiveTableScan tableScan = originalTableScan.copy(originalTableScan.getRowType());
        RelNode projectTableAccessRel = tableScan.project(fieldsUnion, new HashSet<>(0), REL_BUILDER.get());

        relBuilder.push(newInput);
        relBuilder.push(projectTableAccessRel);

        RexNode joinCondition = null;
        boolean first = true;
        for (IntPair keyFieldMapping : sourceTable.getKeyMapping()) {
          int leftKeyIndex = keyFieldMapping.source;
          RelDataTypeField leftKeyField = newInput.getRowType().getFieldList().get(leftKeyIndex);
          int rightKeyIndex = keyFieldMapping.target;
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
      for (Map.Entry<RelOptHiveTable, SourceTable> entry : projectSourceTables.get().entrySet()) {
        RelOptHiveTable relOptHiveTable = entry.getKey();
        for (IntPair fieldMapping : entry.getValue().projectedFieldsMapping) {
          int targetFieldIdx;
          if (offsetMap.containsKey(relOptHiveTable)) {
            Pair<Integer, Mapping> offsetEntry = offsetMap.get(relOptHiveTable);
            targetFieldIdx = offsetEntry.left + offsetEntry.right.getTarget(fieldMapping.target);
          } else {
            targetFieldIdx = trimResult.right.getTarget(fieldMapping.source);
          }

          rootProjectMapping.set(fieldMapping.source, targetFieldIdx);
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
      projectSourceTables.remove();
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

  private static class SourceTable {
    private final List<IntPair> projectedFieldsMapping;
    private final Map<ImmutableBitSet, List<IntPair>> keyMappings;
    private HiveTableScan hiveTableScan;

    private SourceTable() {
      projectedFieldsMapping = new ArrayList<>();
      keyMappings = new HashMap<>();
    }

    private List<IntPair> getKeyMapping() {
      for (Map.Entry<ImmutableBitSet, List<IntPair>> entry : keyMappings.entrySet()) {
        if (entry.getKey().cardinality() == entry.getValue().size()) {
          return entry.getValue();
        }
      }

      return null;
    }
  }
}

