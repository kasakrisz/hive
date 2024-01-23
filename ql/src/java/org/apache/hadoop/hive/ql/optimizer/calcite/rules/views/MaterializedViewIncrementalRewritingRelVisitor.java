/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode.AVAILABLE;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode.INSERT_ONLY;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode.NOT_AVAILABLE;

/**
 * This class is a helper to check whether a materialized view rebuild
 * can be transformed from INSERT OVERWRITE to INSERT INTO.
 *
 * We are verifying that:
 *   1) Plan only uses legal operators (i.e., Filter, Project,
 *   Join, and TableScan)
 *   2) Whether the plane has aggregate
 *   3) Whether the plane has an count(*) aggregate function call
 */
public class MaterializedViewIncrementalRewritingRelVisitor implements ReflectiveVisitor {

  private final ReflectUtil.MethodDispatcher<Result> dispatcher;

  public MaterializedViewIncrementalRewritingRelVisitor() {
    this.dispatcher = ReflectUtil.createMethodDispatcher(
        Result.class, this, "visit", RelNode.class, ImmutableBitSet.class);
  }

  /**
   * Starts an iteration.
   */
  public IncrementalRebuildMode go(RelNode relNode) {
    if (relNode instanceof HiveProject) {
      ImmutableBitSet projectedCols = findProjectedColumnIndexes((HiveProject) relNode);
      Result result = dispatcher.invoke(relNode.getInput(0), projectedCols);
      return result.computeIncrementalRebuildMode();
    } else {
      Result result = dispatcher.invoke(relNode, ImmutableBitSet.of());
      return result.computeIncrementalRebuildMode();
    }
  }

  private ImmutableBitSet findProjectedColumnIndexes(HiveProject project) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (RexNode rexNode : project.getProjects()) {
      if (rexNode instanceof RexInputRef) {
        builder.set(((RexInputRef) rexNode).getIndex());
      }
    }

    return builder.build();
  }

  public Result visit(RelNode relNode, ImmutableBitSet projectedCols) {
    // Only TS, Filter, Join, Project and Aggregate are supported
    return new Result(NOT_AVAILABLE);
  }

  private Result visitChildOf(RelNode rel, ImmutableBitSet projectedCols) {
    return visitChildOf(rel, 0, projectedCols);
  }

  private Result visitChildOf(RelNode rel, int index, ImmutableBitSet projectedCols) {
    return dispatcher.invoke(rel.getInput(index), projectedCols);
  }

  public Result visit(HiveTableScan scan, ImmutableBitSet projectedColPos) {
    RelOptHiveTable hiveTable = (RelOptHiveTable) scan.getTable();

    if (hiveTable.getHiveTableMD().getStorageHandler() != null) {
      if (hiveTable.getHiveTableMD().getStorageHandler().areSnapshotsSupported()) {
        // Incremental rebuild of materialized views with non-native source tables are not implemented
        // when any of the source tables has delete/update operation since the last rebuild
        return new Result(INSERT_ONLY, false, false);
      } else {
        return new Result(NOT_AVAILABLE, false, false);
      }
    }

    boolean uniqueConstraintProjected = primaryKeyProjected(hiveTable, projectedColPos) ||
        anyUniqueKeyProjected(hiveTable, projectedColPos);

    return new Result(AVAILABLE, false, uniqueConstraintProjected);
  }

  private boolean primaryKeyProjected(RelOptHiveTable hiveTable, ImmutableBitSet projectedColPos) {
    PrimaryKeyInfo primaryKeyInfo = hiveTable.getHiveTableMD().getPrimaryKeyInfo();
    if (primaryKeyInfo == null) {
      return false;
    }

    ImmutableBitSet pkColPos = ImmutableBitSet.of(
        primaryKeyInfo.getColNames().values().stream()
            .map(name -> hiveTable.getRowType().getFieldNames().indexOf(name))
            .collect(Collectors.toList()));

    return !pkColPos.isEmpty() && projectedColPos.contains(pkColPos);
  }

  private boolean anyUniqueKeyProjected(RelOptHiveTable hiveTable, ImmutableBitSet projectedColPos) {
    if (hiveTable.getHiveTableMD().getUniqueKeyInfo() == null) {
      return false;
    }

    Collection<List<UniqueConstraint.UniqueConstraintCol>> uniqueConstraints =
        hiveTable.getHiveTableMD().getUniqueKeyInfo().getUniqueConstraints().values();

    for (List<UniqueConstraint.UniqueConstraintCol> uniqueConstraintCols : uniqueConstraints) {
      ImmutableBitSet uniqueColPos = ImmutableBitSet.of(
          uniqueConstraintCols.stream()
              .map(uniqueConstraintCol -> uniqueConstraintCol.position - 1)
              .collect(Collectors.toList()));

      if (!uniqueColPos.isEmpty() && projectedColPos.contains(uniqueColPos)) {
        return true;
      }
    }

    return false;
  }

  public Result visit(HiveProject project, ImmutableBitSet projectedColPos) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int i : projectedColPos) {
      RexNode rexNode = project.getProjects().get(i);
      if (rexNode instanceof RexInputRef) {
        builder.set(((RexInputRef) rexNode).getIndex());
      }
    }
    return visitChildOf(project, builder.build());
  }

  public Result visit(HiveFilter filter, ImmutableBitSet projectedColPos) {
    return visitChildOf(filter, projectedColPos);
  }

  public Result visit(HiveJoin join, ImmutableBitSet projectedColPos) {
    if (join.getJoinType() != JoinRelType.INNER) {
      return new Result(NOT_AVAILABLE);
    }

    ImmutableBitSet.Builder leftBuilder = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightBuilder = ImmutableBitSet.builder();
    int leftColumnCount = join.getLeft().getRowType().getFieldCount();
    for (int i : projectedColPos) {
      if (i < leftColumnCount) {
        leftBuilder.set(i);
      } else {
        rightBuilder.set(i - leftColumnCount);
      }
    }

    Result leftResult = visitChildOf(join, 0, leftBuilder.build());
    Result rightResult = visitChildOf(join, 1, rightBuilder.build());

    boolean containsAggregate = leftResult.containsAggregate || rightResult.containsAggregate;
    boolean uniqueConstraintProjected = leftResult.uniqueConstraintProjected && rightResult.uniqueConstraintProjected;
    switch (rightResult.incrementalRebuildMode) {
      case INSERT_ONLY:
        return new Result(INSERT_ONLY, containsAggregate, uniqueConstraintProjected);
      case AVAILABLE:
        return new Result(
            leftResult.incrementalRebuildMode == INSERT_ONLY ? INSERT_ONLY : AVAILABLE,
            containsAggregate,
            uniqueConstraintProjected);
      case NOT_AVAILABLE:
      case UNKNOWN:
      default:
        return new Result(rightResult.incrementalRebuildMode, containsAggregate, uniqueConstraintProjected);
    }
  }

  public Result visit(HiveAggregate aggregate, ImmutableBitSet projectedColPos) {
    Result result = visitChildOf(aggregate, projectedColPos);
    if (result.incrementalRebuildMode != AVAILABLE) {
      return new Result(result.incrementalRebuildMode, true, result.uniqueConstraintProjected);
    }

    Map<Integer, Set<SqlKind>> columnRefByAggregateCall = new HashMap<>(aggregate.getRowType().getFieldCount());

    boolean hasCountStar = false;
    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.getArgList().isEmpty()) {
        hasCountStar = true;
        continue;
      }

      for (Integer argIndex : aggregateCall.getArgList()) {
        columnRefByAggregateCall.putIfAbsent(argIndex, new HashSet<>());
        Set<SqlKind> aggregates = columnRefByAggregateCall.get(argIndex);
        aggregates.add(aggregateCall.getAggregation().getKind());
      }
    }

    IncrementalRebuildMode incrementalRebuildMode = hasCountStar ? AVAILABLE : INSERT_ONLY;
    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
        case SUM:
        case SUM0:
          break;

        case AVG:
          Set<SqlKind> aggregates = columnRefByAggregateCall.get(aggregateCall.getArgList().get(0));
          if (!(aggregates.contains(SqlKind.SUM) && aggregates.contains(SqlKind.COUNT))) {
            incrementalRebuildMode = NOT_AVAILABLE;
          }
          break;

        default:
          incrementalRebuildMode = NOT_AVAILABLE;
          break;
      }
    }

    return new Result(incrementalRebuildMode, true, result.uniqueConstraintProjected);
  }

  public static class Result {
    final IncrementalRebuildMode incrementalRebuildMode;
    final boolean containsAggregate;
    final boolean uniqueConstraintProjected;

    public Result(IncrementalRebuildMode incrementalRebuildMode) {
      this(incrementalRebuildMode, false, true);
    }

    public Result(
        IncrementalRebuildMode incrementalRebuildMode,
        boolean containsAggregate,
        boolean uniqueConstraintProjected) {
      this.incrementalRebuildMode = incrementalRebuildMode;
      this.containsAggregate = containsAggregate;
      this.uniqueConstraintProjected = uniqueConstraintProjected;
    }

    public IncrementalRebuildMode computeIncrementalRebuildMode() {
      if (containsAggregate) {
        return incrementalRebuildMode;
      }

      if (incrementalRebuildMode != AVAILABLE) {
        return incrementalRebuildMode;
      }

      return uniqueConstraintProjected ? AVAILABLE : INSERT_ONLY;
    }
  }
}
