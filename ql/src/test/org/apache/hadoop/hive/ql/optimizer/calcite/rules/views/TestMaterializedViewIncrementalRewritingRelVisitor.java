package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TestMaterializedViewIncrementalRewritingRelVisitor extends TestRuleBase {
  @Test
  public void test() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);

    RelNode mvQueryPlan = relBuilder
        .push(ts1)
        .sort(1)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    visitor.go(mvQueryPlan);
  }
}