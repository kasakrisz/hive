package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class TestMaterializedViewIncrementalRewritingRelVisitor extends TestRuleBase {
  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasUnsupportedOperator() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);

    RelNode mvQueryPlan = relBuilder
        .push(ts1)
        .sort(1) // Order by is not supported
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsAvailableWhenPlanHasProject() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);

    RelNode mvQueryPlan = relBuilder
        .push(ts1)
        .project(
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(1).getType(), 1))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan), is(IncrementalRebuildMode.AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsAvailableWhenPlanHasFilter() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);

    RelNode mvQueryPlan = relBuilder
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan), is(IncrementalRebuildMode.AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsAvailableWhenPlanHasInnerJoin() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    RelNode mvQueryPlan = relBuilder
        .push(ts1)
        .push(ts2)
        .join(JoinRelType.INNER, joinCondition)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan), is(IncrementalRebuildMode.AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasJoinOtherThanInner() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    RelNode mvQueryPlan = relBuilder
        .push(ts1)
        .push(ts2)
        .join(JoinRelType.LEFT, joinCondition)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }
}