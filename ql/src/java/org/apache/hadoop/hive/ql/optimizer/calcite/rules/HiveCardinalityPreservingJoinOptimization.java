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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HiveCardinalityPreservingJoinOptimization extends HiveRelFieldTrimmer {

  private final ThreadLocal<Stack<ImmutableBitSet>> fieldsProjectUsedStack;
  private final ThreadLocal<List<TableAccessRelEntry>> tableAccessRelList;

  public HiveCardinalityPreservingJoinOptimization() {
    super(false);
    fieldsProjectUsedStack = ThreadLocal.withInitial(Stack::new);
    tableAccessRelList = ThreadLocal.withInitial(ArrayList::new);
  }

  @Override
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    REL_BUILDER.set(relBuilder);

    HiveProject rootProject;
    RelNode relNode = root;
    while (!(relNode instanceof HiveProject)) {
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
    ImmutableBitSet rootProjectFieldsUsed = inputFinder.inputBitSet.build();


    fieldsProjectUsedStack.get().push(rootProjectFieldsUsed);
    RelNode rootProjectInput = rootProject.getInput(0);

    ImmutableBitSet fieldsUsed = ImmutableBitSet.of();
    Set<RelDataTypeField> extraFields = Collections.emptySet();
    final TrimResult trimResult = dispatchTrimFields(rootProjectInput, fieldsUsed, extraFields);

    fieldsProjectUsedStack.get().pop();

//    RelNode trimResult = super.trim(relBuilder, relNode.getInput(0));

    if (!tableAccessRelList.get().isEmpty()) {
      final RexBuilder rexBuilder = REL_BUILDER.get().getRexBuilder();

      int i = 0;
      RelNode newInput = trimResult.left;
      Mapping inputMapping = Mappings.create(
          MappingType.INVERSE_SURJECTION, rootProjectInput.getRowType().getFieldCount(),
          fieldCount);
      for (IntPair pair : trimResult.right) {
        if (rootProjectFieldsUsed.get(pair.source)) {
          inputMapping.set(pair.source, pair.target);
        }
      }


//      Mapping inputMapping = trimResult.right;
      int offset = 0;
      int newOffset = trimResult.right.getTargetCount();

      for (TableAccessRelEntry tableAccessRel : tableAccessRelList.get()) {
        relBuilder.push(newInput);
        relBuilder.push(tableAccessRel.getRelNode());

        // TODO: composite keys
        int leftKeyIndex = i;
        RelDataTypeField leftKeyField = newInput.getRowType().getFieldList().get(leftKeyIndex);
        int rightKeyIndex = tableAccessRel.getKeyMapping().getSource(0);
        RelDataTypeField rightKeyField = tableAccessRel.getRelNode().getRowType().getFieldList().get(rightKeyIndex);

        RexNode joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
            rexBuilder.makeInputRef(rightKeyField.getValue(),
                newInput.getRowType().getFieldCount() + rightKeyIndex));

        Mapping newMapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            trimResult.right.getSourceCount() + tableAccessRel.projectMapping.getSourceCount(),
            trimResult.right.getTargetCount() + rootProject.getProjects().size());

        for (IntPair pair : inputMapping) {
          newMapping.set(pair.source, pair.target);
        }

//        for (int j = 0; j < inputMapping.getTargetCount(); ++j) {
//          int targetOpt = inputMapping.getTargetOpt(j);
//          if (targetOpt != -1) {
//            newMapping.set(j, targetOpt);
//          }
//        }

        for (IntPair pair : tableAccessRel.projectMapping) {
          newMapping.set(pair.source + offset, pair.target + newOffset);
        }

//        for (int j = 0; j < tableAccessRel.projectMapping.getTargetCount(); ++j) {
//          int targetOpt = tableAccessRel.projectMapping.getTargetOpt(j);
//          if (targetOpt != -1) {
//            newMapping.set(j + offset, targetOpt + newOffset);
//          }
//        }

        inputMapping = newMapping;
        newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
        ++i;
        offset += tableAccessRel.projectMapping.getTargetCount();
        newOffset += tableAccessRel.projectMapping.getTargetCount();
      }
    }

    return trimResult.left;
  }

  @Override
  public TrimResult trimFields(Join join, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    RelOptUtil.InputFinder projectInputFinder = new RelOptUtil.InputFinder();
    projectInputFinder.inputBitSet.addAll(fieldsProjectUsedStack.get().peek());
    final ImmutableBitSet fieldsProjectUsedPlus = projectInputFinder.inputBitSet.build();

    Stack<ImmutableBitSet> inputFieldsUsed = new Stack<>();

    int offset = 0;
    for (RelNode input : join.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();
      ImmutableBitSet.Builder inputFieldsProjectUsed = ImmutableBitSet.builder();
      for (int bit : fieldsProjectUsedPlus) {
        if (bit >= offset && bit < offset + inputFieldCount) {
          inputFieldsProjectUsed.set(bit - offset);
        }
      }
      inputFieldsUsed.push(inputFieldsProjectUsed.build());
      offset += inputFieldCount;
    }

    while (!inputFieldsUsed.empty()) {
      fieldsProjectUsedStack.get().push(inputFieldsUsed.pop());
    }

    return super.trimFields(join, fieldsUsed, extraFields);
  }

  @Override
  public TrimResult trimFields(
      HiveTableScan tableAccessRel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    TrimResult result = super.trimFields(tableAccessRel, fieldsUsed, extraFields);

    if (!fieldsUsed.equals(fieldsProjectUsedStack.get().peek())) {
      ImmutableBitSet fieldProjectUsed = fieldsProjectUsedStack.get().pop();
      ImmutableBitSet fieldUnion = fieldProjectUsed.union(fieldsUsed);
      HiveTableScan tableScan = tableAccessRel.copy(tableAccessRel.getRowType());
      RelNode projectTableAccessRel = tableScan.project(fieldUnion, new HashSet<>(0), REL_BUILDER.get());
      final Mapping projectMapping = createMapping(fieldUnion, tableScan.getRowType().getFieldCount());
      final Mapping keyMapping = createMapping(fieldsUsed, tableScan.getRowType().getFieldCount());
      tableAccessRelList.get().add(new TableAccessRelEntry(projectTableAccessRel, keyMapping, projectMapping));
    }

    return result;
  }

  protected static class TableAccessRelEntry {
    private final RelNode relNode;
    private final Mapping keyMapping;
    private final Mapping projectMapping;

    public TableAccessRelEntry(RelNode relNode, Mapping keyMapping, Mapping projectMapping) {
      this.relNode = relNode;
      this.keyMapping = keyMapping;
      this.projectMapping = projectMapping;
    }

    public RelNode getRelNode() {
      return relNode;
    }

    public Mapping getKeyMapping() {
      return keyMapping;
    }

    public Mapping getProjectMapping() {
      return projectMapping;
    }
  }
}

