package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.derby.vti.XmlVTI.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;

class TestMaterializedViews {
  private static final MaterializedViews materializedViews = new MaterializedViews();

  @Test
  void testAdd() {
    Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    table.setDbName("default");
    table.setTableName("mat1");
    table.setViewOriginalText("select col0 from t1");
    RelOptMaterialization relOptMaterialization = new RelOptMaterialization(
            new DummyRel(), new DummyRel(), null, asList(table.getDbName(), table.getTableName()));
    materializedViews.putIfAbsent(table, relOptMaterialization);

    assertThat(materializedViews.get(table.getViewOriginalText()), is(relOptMaterialization));
  }

  private static List<Pair<Table, RelOptMaterialization>> testData = new ArrayList<>();

  @BeforeAll
  static void beforeAll() {
    for (int i = 0; i < 10; ++i) {
      Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
      table.setDbName("default");
      table.setTableName("mat" + i);
      table.setViewOriginalText("select col0 from t" + i);
      RelOptMaterialization relOptMaterialization = new RelOptMaterialization(
              new DummyRel(), new DummyRel(), null, asList(table.getDbName(), table.getTableName()));
      testData.add(new Pair<>(table, relOptMaterialization));
    }
    for (int i = 0; i < 10; ++i) {
      Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
      table.setDbName("default2");
      table.setTableName("mat" + i);
      table.setViewOriginalText("select col0 from t" + i);
      RelOptMaterialization relOptMaterialization = new RelOptMaterialization(
              new DummyRel(), new DummyRel(), null, asList(table.getDbName(), table.getTableName()));
      testData.add(new Pair<>(table, relOptMaterialization));
    }
  }

  @Test
  void testParallelism() {
    int ITERATIONS = 100000;

    List<Callable<Void>> callableList = new ArrayList<>();
    callableList.add(() -> {
      refreshAll();
      return null;
    });
    callableList.add(() -> {
      for (int j = 0; j < ITERATIONS; ++j) {
        removeThenAdd();
      }
      return null;
    });
    for (Pair<Table, RelOptMaterialization> entry : testData) {
      callableList.add(() -> {
        for (int j = 0; j < ITERATIONS; ++j) {
          materializedViews.get(entry.left.getViewOriginalText());
        }
        return null;
      });
    }
    callableList.add(() -> {
      for (int j = 0; j < ITERATIONS; ++j) {
        List<RelOptMaterialization> materializations = materializedViews.values();
      }
      return null;
    });


    ExecutorService executor = Executors.newFixedThreadPool(12);
    try {
      executor.invokeAll(callableList);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void refreshAll() {
    for (Pair<Table, RelOptMaterialization> entry : testData) {
      materializedViews.refresh(entry.left, entry.left, entry.right);
    }
  }

  private void removeThenAdd() {
    for (Pair<Table, RelOptMaterialization> entry : testData) {
      materializedViews.remove(entry.left);
      materializedViews.putIfAbsent(entry.left, entry.right);
    }
  }

  private static class DummyRel implements RelNode {

    @Override
    public List<RexNode> getChildExps() {
      return null;
    }

    @Override
    public Convention getConvention() {
      return null;
    }

    @Override
    public String getCorrelVariable() {
      return null;
    }

    @Override
    public boolean isDistinct() {
      return false;
    }

    @Override
    public RelNode getInput(int i) {
      return null;
    }

    @Override
    public RelOptQuery getQuery() {
      return null;
    }

    @Override
    public int getId() {
      return 0;
    }

    @Override
    public String getDigest() {
      return null;
    }

    @Override
    public RelTraitSet getTraitSet() {
      return null;
    }

    @Override
    public RelDataType getRowType() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public RelDataType getExpectedInputRowType(int i) {
      return null;
    }

    @Override
    public List<RelNode> getInputs() {
      return null;
    }

    @Override
    public RelOptCluster getCluster() {
      return null;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery relMetadataQuery) {
      return 0;
    }

    @Override
    public double getRows() {
      return 0;
    }

    @Override
    public Set<String> getVariablesStopped() {
      return null;
    }

    @Override
    public Set<CorrelationId> getVariablesSet() {
      return null;
    }

    @Override
    public void collectVariablesUsed(Set<CorrelationId> set) {

    }

    @Override
    public void collectVariablesSet(Set<CorrelationId> set) {

    }

    @Override
    public void childrenAccept(RelVisitor relVisitor) {

    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner relOptPlanner, RelMetadataQuery relMetadataQuery) {
      return null;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner relOptPlanner) {
      return null;
    }

    @Override
    public <M extends Metadata> M metadata(Class<M> aClass, RelMetadataQuery relMetadataQuery) {
      return null;
    }

    @Override
    public void explain(RelWriter relWriter) {

    }

    @Override
    public RelNode onRegister(RelOptPlanner relOptPlanner) {
      return null;
    }

    @Override
    public String recomputeDigest() {
      return null;
    }

    @Override
    public void replaceInput(int i, RelNode relNode) {

    }

    @Override
    public RelOptTable getTable() {
      return null;
    }

    @Override
    public String getRelTypeName() {
      return null;
    }

    @Override
    public boolean isValid(Litmus litmus, Context context) {
      return false;
    }

    @Override
    public boolean isValid(boolean b) {
      return false;
    }

    @Override
    public List<RelCollation> getCollationList() {
      return null;
    }

    @Override
    public RelNode copy(RelTraitSet relTraitSet, List<RelNode> list) {
      return null;
    }

    @Override
    public void register(RelOptPlanner relOptPlanner) {

    }

    @Override
    public boolean isKey(ImmutableBitSet immutableBitSet) {
      return false;
    }

    @Override
    public RelNode accept(RelShuttle relShuttle) {
      return null;
    }

    @Override
    public RelNode accept(RexShuttle rexShuttle) {
      return null;
    }
  }
}