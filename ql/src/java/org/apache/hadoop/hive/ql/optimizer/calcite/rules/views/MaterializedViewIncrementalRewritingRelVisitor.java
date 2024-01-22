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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

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

  protected final Deque<RelNode> stack = new ArrayDeque<>();
  private final ReflectUtil.MethodDispatcher<IncrementalRebuildMode> dispatcher;

//  private boolean containsAggregate;
//  private boolean hasAllowedOperatorsOnly;
//  private boolean hasCountStar;
//  private boolean insertAllowedOnly;

  public MaterializedViewIncrementalRewritingRelVisitor() {
    this.dispatcher = ReflectUtil.createMethodDispatcher(
        IncrementalRebuildMode.class, this, "visit", RelNode.class);

//    this.containsAggregate = false;
//    this.hasAllowedOperatorsOnly = true;
//    this.hasCountStar = false;
//    this.insertAllowedOnly = false;
  }

  /**
   * Starts an iteration.
   */
  public IncrementalRebuildMode go(RelNode relNode) {
    return dispatcher.invoke(relNode);
  }

  public IncrementalRebuildMode visit(RelNode relNode) {
    // Only TS, Filter, Join, Project and Aggregate are supported
    return IncrementalRebuildMode.NOT_AVAILABLE;
  }

  protected IncrementalRebuildMode visitChildren(RelNode rel) {
    IncrementalRebuildMode incrementalRebuildMode = IncrementalRebuildMode.UNKNOWN;
    for (RelNode child : rel.getInputs()) {
      incrementalRebuildMode = dispatcher.invoke(child);
      if (incrementalRebuildMode == IncrementalRebuildMode.NOT_AVAILABLE ||
          incrementalRebuildMode == IncrementalRebuildMode.UNKNOWN) {
        return incrementalRebuildMode;
      }
    }
    return incrementalRebuildMode;
  }

  public IncrementalRebuildMode visit(HiveTableScan scan) {
    RelOptHiveTable hiveTable = (RelOptHiveTable) scan.getTable();
    if (hiveTable.getHiveTableMD().getStorageHandler() != null &&
        hiveTable.getHiveTableMD().getStorageHandler().areSnapshotsSupported()) {
      // Incremental rebuild of materialized views with non-native source tables are not implemented
      // when any of the source tables has delete/update operation since the last rebuild
      return IncrementalRebuildMode.INSERT_ONLY;
    }
    return IncrementalRebuildMode.AVAILABLE;
  }

  public IncrementalRebuildMode visit(HiveProject project) {
    return visitChildren(project);
  }

  public IncrementalRebuildMode visit(HiveFilter filter) {
    return visitChildren(filter);
  }

  public IncrementalRebuildMode visit(HiveJoin join) {
    if (join.getJoinType() == JoinRelType.INNER) {
      return visitChildren(join);
    }
    return IncrementalRebuildMode.NOT_AVAILABLE;
  }

//  @Override
//  public void visit(RelNode node, int ordinal, RelNode parent) {
//    if (node instanceof Aggregate) {
//      this.containsAggregate = true;
//      check((Aggregate) node);
//      super.visit(node, ordinal, parent);
//    } else if (
//            node instanceof Filter ||
//            node instanceof Project ||
//            node instanceof Join) {
//      super.visit(node, ordinal, parent);
//    } else if (node instanceof TableScan) {
//      HiveTableScan scan = (HiveTableScan) node;
//      RelOptHiveTable hiveTable = (RelOptHiveTable) scan.getTable();
//      if (hiveTable.getHiveTableMD().getStorageHandler() != null &&
//              hiveTable.getHiveTableMD().getStorageHandler().areSnapshotsSupported()) {
//        // Incremental rebuild of materialized views with non-native source tables are not implemented
//        // when any of the source tables has delete/update operation since the last rebuild
//        insertAllowedOnly = true;
//      }
//    } else {
//      hasAllowedOperatorsOnly = false;
//    }
//  }

//  private void check(Aggregate aggregate) {
//    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
//      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
//      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.getArgList().size() == 0) {
//        hasCountStar = true;
//        break;
//      }
//    }
//  }
}
