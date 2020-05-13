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
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
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
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HiveCardinalityPreservingJoinOptimization extends HiveRelFieldTrimmer {

  private final ThreadLocal<List<TableAccessRelEntry>> tableAccessRelList;

  public HiveCardinalityPreservingJoinOptimization() {
    super(false);
    tableAccessRelList = ThreadLocal.withInitial(ArrayList::new);
  }

  @Override
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    REL_BUILDER.set(relBuilder);

    HiveProject rootProject;
    RelNode relNode = root;
    while (!(relNode instanceof HiveProject)) {
      if (relNode.getInputs().size() != 1) {
        return root;
      }
      relNode = relNode.getInput(0);
    }

    rootProject = (HiveProject) relNode;
    int fieldCount = rootProject.getRowType().getFieldCount();

    // Which fields are required from the input?
    ImmutableBitSet tmp = ImmutableBitSet.range(fieldCount);
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder();
    for (Ord<RexNode> ord : Ord.zip(rootProject.getProjects())) {
      if (tmp.get(ord.i)) {
        ord.e.accept(inputFinder);
      }
    }

    RelNode rootProjectInput = rootProject.getInput(0);
    Map<RelOptHiveTable, List<IntPair>> rootProjectFieldSourceMap = new HashMap<>();
    ImmutableBitSet projectedFields = ImmutableBitSet.of();
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

      List<IntPair> fieldsOfTable = rootProjectFieldSourceMap.computeIfAbsent(relOptHiveTable, k -> new ArrayList<>());
      fieldsOfTable.add(new IntPair(projectExpr.getIndex(), rexTableInputRef.getIndex()));
    }

    ImmutableBitSet fieldsUsed = ImmutableBitSet.of();
    Set<RelDataTypeField> extraFields = Collections.emptySet();
    final TrimResult trimResult = dispatchTrimFields(rootProjectInput, fieldsUsed, extraFields);

    if (tableAccessRelList.get().isEmpty()) {
      return root;
    }

    final RexBuilder rexBuilder = REL_BUILDER.get().getRexBuilder();

    int i = 0;
    RelNode newInput = trimResult.left;
    Map<RelOptHiveTable, Integer> offsetMap = new HashMap<>();
    for (TableAccessRelEntry tableAccessRelEntry : tableAccessRelList.get()) {
      RelOptHiveTable relOptHiveTable = (RelOptHiveTable) tableAccessRelEntry.tableScan.getTable();
      offsetMap.put(relOptHiveTable, newInput.getRowType().getFieldCount());
      List<IntPair> fieldMappings = rootProjectFieldSourceMap.get(relOptHiveTable);

      ImmutableBitSet fieldsProjected = ImmutableBitSet.of();
      for (IntPair fieldMapping : fieldMappings) {
        fieldsProjected = fieldsProjected.set(fieldMapping.target);
      }
      ImmutableBitSet fieldsUnion = fieldsProjected.union(tableAccessRelEntry.keyFields);

      HiveTableScan tableScan = tableAccessRelEntry.tableScan.copy(tableAccessRelEntry.tableScan.getRowType());
      RelNode projectTableAccessRel = tableScan.project(fieldsUnion, new HashSet<>(0), REL_BUILDER.get());

      relBuilder.push(newInput);
      relBuilder.push(projectTableAccessRel);

      // TODO: composite keys
      int leftKeyIndex = i;
      RelDataTypeField leftKeyField = newInput.getRowType().getFieldList().get(leftKeyIndex);
      int rightKeyIndex = tableAccessRelEntry.keyFields.iterator().next();
      RelDataTypeField rightKeyField = tableScan.getRowType().getFieldList().get(rightKeyIndex);

      RexNode joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
          rexBuilder.makeInputRef(rightKeyField.getValue(),
              newInput.getRowType().getFieldCount() + rightKeyIndex));

      newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
      ++i;
    }

    final Mapping rootProjectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
        newInput.getRowType().getFieldCount(), newInput.getRowType().getFieldCount());
    for (Map.Entry<RelOptHiveTable, List<IntPair>> entry : rootProjectFieldSourceMap.entrySet()) {
      RelOptHiveTable relOptHiveTable = entry.getKey();
      for (IntPair fieldMapping : entry.getValue()) {
        int targetFieldIdx;
        if (offsetMap.containsKey(relOptHiveTable)) {
          targetFieldIdx = offsetMap.get(relOptHiveTable) + fieldMapping.target;
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

    return relBuilder.build();
  }

  @Override
  public TrimResult trimFields(
      HiveTableScan tableAccessRel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    TrimResult result = super.trimFields(tableAccessRel, fieldsUsed, extraFields);
    tableAccessRelList.get().add(new TableAccessRelEntry(tableAccessRel, fieldsUsed));
    return result;
  }

  protected static class TableAccessRelEntry {
    private final HiveTableScan tableScan;
    private final ImmutableBitSet keyFields;

    public TableAccessRelEntry(HiveTableScan tableScan, ImmutableBitSet keyFields) {
      this.tableScan = tableScan;
      this.keyFields = keyFields;
    }

    public RelNode getTableScan() {
      return tableScan;
    }

    public ImmutableBitSet getKeyFields() {
      return keyFields;
    }
  }
}

