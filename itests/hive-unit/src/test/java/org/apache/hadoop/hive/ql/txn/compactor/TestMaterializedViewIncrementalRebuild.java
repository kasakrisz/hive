/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.execSelectAndDumpData;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;

public class TestMaterializedViewIncrementalRebuild extends CompactorOnTezTest {

  private final static String TABLE1 = "t1";
  private final static String MV1 = "mat1";

  @Override
  public void tearDown() {

    dropMaterializedView(MV1);
    dropTable(TABLE1);

    super.tearDown();
  }

  @Test
  public void testIncrementalMVRebuild() throws Exception {
    createTransactionalTable(TABLE1);
    insertTestDataTo(TABLE1);
    createTransactionalMaterializedView(MV1);

    executeStatementOnDriver("delete from "+ TABLE1 + " where a = 1", driver);

    CompactorTestUtil.runCompaction(conf, "default", TABLE1, CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);

    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    List<String> result = execSelectAndDumpData("select * from " + MV1, driver, "");
    Assert.assertEquals(Arrays.asList("2\ttwo\t2.2", "NULL\tNULL\tNULL"), result);

    result = execSelectAndDumpData("explain cbo select a,b,c from " + TABLE1 + " where a > 0 or a is null",
            driver, "");
    Assert.assertEquals(Arrays.asList(
            "CBO PLAN:", "HiveTableScan(table=[[default, " + MV1 + "]], table:alias=[default." + MV1 + "])", ""),
            result);
  }

  private void createTransactionalTable(String tableName) throws Exception {
    executeStatementOnDriver("create table "+ tableName + "(a int, b varchar(128), c float) " +
            "stored as orc TBLPROPERTIES ('transactional'='true')", driver);
  }

  private void insertTestDataTo(String tableName) throws Exception {
    executeStatementOnDriver("insert into " + tableName + "(a,b, c) " +
            "values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL)", driver);
  }

  private void createTransactionalMaterializedView(String viewName) throws Exception {
    executeStatementOnDriver("create materialized view " + viewName +
            " stored as orc TBLPROPERTIES ('transactional'='true') as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);
  }

  private void dropTable(String tableName) {
    try {
      executeStatementOnDriver("drop table " + tableName, driver);
    } catch (Exception ignore) {

    }
  }

  private void dropMaterializedView(String tableName) {
    try {
      executeStatementOnDriver("drop materialized view " + tableName, driver);
    } catch (Exception ignore) {

    }
  }

  @Test
  public void testIncrementalMVRebuildIfCleanUpHasNotFinished() throws Exception {
    createTransactionalTable(TABLE1);
    insertTestDataTo(TABLE1);
    createTransactionalMaterializedView(MV1);

    executeStatementOnDriver("delete from " + TABLE1 + " where a = 1", driver);

    CompactorTestUtil.runCompaction(conf, "default", "" + TABLE1 + "", CompactionType.MAJOR, true);

    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    List<String> result = execSelectAndDumpData("select * from " + MV1 + "", driver, "");
    Assert.assertEquals(Arrays.asList("2\ttwo\t2.2", "NULL\tNULL\tNULL"), result);

    result = execSelectAndDumpData("explain cbo select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, " + MV1 + "]], table:alias=[default." + MV1 + "])", ""), result);
  }

}
