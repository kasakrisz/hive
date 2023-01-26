/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergTestUtils.timestampAfterSnapshot;

/**
 * Tests covering the time travel feature, aka reading from a table as of a certain snapshot.
 */
public class TestHiveIcebergTimeTravel extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testSelectAsOfTimestamp() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    List<Object[]> rows = shell.executeStatement(
        "SELECT * FROM customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 0) + "'");

    Assert.assertEquals(3, rows.size());

    rows = shell.executeStatement(
        "SELECT * FROM customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "'");

    Assert.assertEquals(4, rows.size());

    try {
      shell.executeStatement("SELECT * FROM customers FOR SYSTEM_TIME AS OF '1970-01-01 00:00:00'");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Cannot find a snapshot older than 1970-01-01"));
    }
  }

  @Test
  public void testSelectAsOfVersion() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    HistoryEntry first = table.history().get(0);
    List<Object[]> rows =
        shell.executeStatement("SELECT * FROM customers FOR SYSTEM_VERSION AS OF " + first.snapshotId());

    Assert.assertEquals(3, rows.size());

    HistoryEntry second = table.history().get(1);
    rows = shell.executeStatement("SELECT * FROM customers FOR SYSTEM_VERSION AS OF " + second.snapshotId());

    Assert.assertEquals(4, rows.size());

    try {
      shell.executeStatement("SELECT * FROM customers FOR SYSTEM_VERSION AS OF 1234");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Cannot find snapshot with ID 1234"));
    }
  }

  @Test
  public void testCTASAsOfVersionAndTimestamp() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 3);

    shell.executeStatement("CREATE TABLE customers2 AS SELECT * FROM customers FOR SYSTEM_VERSION AS OF " +
        table.history().get(0).snapshotId());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM customers2");
    Assert.assertEquals(3, rows.size());

    shell.executeStatement("INSERT INTO customers2 SELECT * FROM customers FOR SYSTEM_VERSION AS OF " +
        table.history().get(1).snapshotId());

    rows = shell.executeStatement("SELECT * FROM customers2");
    Assert.assertEquals(7, rows.size());

    shell.executeStatement("CREATE TABLE customers3 AS SELECT * FROM customers FOR SYSTEM_TIME AS OF '" +
        timestampAfterSnapshot(table, 1) + "'");

    rows = shell.executeStatement("SELECT * FROM customers3");
    Assert.assertEquals(4, rows.size());

    shell.executeStatement("INSERT INTO customers3 SELECT * FROM customers FOR SYSTEM_TIME AS OF '" +
        timestampAfterSnapshot(table, 0) + "'");

    rows = shell.executeStatement("SELECT * FROM customers3");
    Assert.assertEquals(7, rows.size());
  }

  @Test
  public void testSelectAsOfCurrentTimestampAndInterval() throws IOException, InterruptedException {
    testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 3);

    List<Object[]> rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP + interval '10' hours");

    Assert.assertEquals(5, rows.size());
  }

  @Test
  public void testInvalidSelectAsOfTimestampExpression() throws IOException, InterruptedException {
    Table icebergTable = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 3);
    icebergTable.updateSchema().addColumn("create_time", Types.TimestampType.withZone()).commit();

    Assert.assertThrows(IllegalArgumentException.class, () -> shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF create_time - interval '10' hours"));
  }

  @Test
  public void testAsOfWithJoins() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 4);

    List<Object[]> rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 0) + "' fv, " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "' sv " +
        "WHERE fv.first_name=sv.first_name");

    Assert.assertEquals(4, rows.size());

    rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "' sv, " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 2) + "' tv " +
        "WHERE sv.first_name=tv.first_name");

    Assert.assertEquals(8, rows.size());

    rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 2) + "' sv, " +
        "customers lv " +
        "WHERE sv.first_name=lv.first_name");

    Assert.assertEquals(14, rows.size());

    rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "' sv, " +
        "customers FOR SYSTEM_VERSION AS OF " + table.history().get(2).snapshotId() + " tv " +
        "WHERE sv.first_name=tv.first_name");

    Assert.assertEquals(8, rows.size());
  }

  private Table prepareTableWithVersions(int versionCount) throws IOException, InterruptedException {
    return testTables.createTableWithVersions(shell, "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, versionCount);
  }

  @Test
  public void testStartEndTimeWithTimesInTheMiddle() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    String start = timestampAfterSnapshot(table, 1);
    String end = timestampAfterSnapshot(table, 3);
    List<Object[]> rows = shell.executeStatement("select * from customers for system_time start " +
            "'" + start + "' end '" + end + "' ORDER BY last_name");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("Green_1", rows.get(0)[2]);
    Assert.assertEquals("Green_2", rows.get(1)[2]);
  }

  @Test
  public void testStartEndTimeExpressions() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    String start = timestampAfterSnapshot(table, 1);
    List<Object[]> rows = shell.executeStatement("select * from customers for system_time start " +
            " cast('" + start + "' as timestamp) - interval '1' seconds end CURRENT_TIMESTAMP + interval '10' hours " +
            "ORDER BY last_name");
    Assert.assertEquals(4, rows.size());
  }

  @Test
  public void testStartEndTimeWithStartTimeBeforeTableCreation() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    String end = timestampAfterSnapshot(table, 3);
    List<Object[]> rows = shell.executeStatement("select * from customers for system_time start '1999-01-01 00:00:00'" +
            " end '" + end + "' ORDER BY last_name");
    // 1999-01-01 00:00:00 translates into the first snapshot of the table, however since fromSnapshotId is always
    // exclusive in TableScan#appendsBetween, the first 3 records are excluded from the resultset
    // this is not very intuitive, but sadly there is currently no API support for left-inclusive time travel ranges
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("Green_0", rows.get(0)[2]);
    Assert.assertEquals("Green_1", rows.get(1)[2]);
    Assert.assertEquals("Green_2", rows.get(2)[2]);
  }

  @Test
  public void testStartEndTimeWithToTimeInTheFuture() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    String start = timestampAfterSnapshot(table, 1);
    List<Object[]> rows = shell.executeStatement("select * from customers for system_time start " +
            "'" + start + "' end '2060-01-01 00:00:00' ORDER BY last_name");
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("Green_1", rows.get(0)[2]);
    Assert.assertEquals("Green_2", rows.get(1)[2]);
    Assert.assertEquals("Green_3", rows.get(2)[2]);
  }

  @Test
  public void testStartTimeWithoutEndClause() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    String start = timestampAfterSnapshot(table, 2);
    List<Object[]> rows = shell.executeStatement("select * from customers for system_time start " +
            "'" + start + "' ORDER BY last_name");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("Green_2", rows.get(0)[2]);
    Assert.assertEquals("Green_3", rows.get(1)[2]);
  }

  @Test
  public void testCTASFromEndTime() throws IOException, InterruptedException {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);
    Table table = prepareTableWithVersions(5);
    String start = timestampAfterSnapshot(table, 1);
    String end = timestampAfterSnapshot(table, 3);
    shell.executeStatement(String.format("create table customers2 stored by iceberg stored as %s as select * " +
            "from customers for system_time start '%s' end '%s' ORDER BY last_name", fileFormat, start, end));
    List<Object[]> rows = shell.executeStatement("SELECT * FROM customers2 ORDER BY last_name");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("Green_1", rows.get(0)[2]);
    Assert.assertEquals("Green_2", rows.get(1)[2]);
  }

  @Test
  public void testStartTimeInTheFuture() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    AssertHelpers.assertThrows("Should throw error for future date in FROM clause", IllegalArgumentException.class,
            "Provided FROM timestamp must be earlier than the commit time of latest snapshot of the table.",
        () -> shell.executeStatement("select * from customers for system_time start '2060-01-01 00:00:00'"));
  }

  @Test
  public void testEndTimeInThePastBeforeTableCreation() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    AssertHelpers.assertThrows("Should throw error for future date in FROM clause", IllegalArgumentException.class,
            "Provided TO timestamp must be after the commit time of the first snapshot of the table.",
        () -> shell.executeStatement("select * from customers for system_time start '1981-01-01 00:00:00' end " +
                    "'1993-01-01 00:00:00'"));
  }

  @Test
  public void testEndTimeEarlierThanStartTime() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    AssertHelpers.assertThrows("Should throw error for future date in FROM clause", IllegalArgumentException.class,
            "Provided FROM timestamp must precede the provided TO timestamp.",
        () -> shell.executeStatement("select * from customers for system_time start '1999-01-01 00:00:00' end " +
                    "'1993-01-01 00:00:00'"));
  }

  @Test
  public void testStartEndVersionWithVersionsInTheMiddle() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    long start = table.history().get(1).snapshotId();
    long end = table.history().get(3).snapshotId();
    List<Object[]> rows = shell.executeStatement(
            "select * from customers for system_version start " + start + " end " + end + " ORDER BY last_name");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("Green_1", rows.get(0)[2]);
    Assert.assertEquals("Green_2", rows.get(1)[2]);
  }

  @Test
  public void testStartVersionWithoutEndClause() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    long start = table.history().get(0).snapshotId();
    List<Object[]> rows = shell.executeStatement(
            "select * from customers for system_version start " + start + " ORDER BY last_name");
    Assert.assertEquals(4, rows.size());
    Assert.assertEquals("Green_0", rows.get(0)[2]);
    Assert.assertEquals("Green_1", rows.get(1)[2]);
    Assert.assertEquals("Green_2", rows.get(2)[2]);
    Assert.assertEquals("Green_3", rows.get(3)[2]);
  }

  @Test
  public void testCTASStartEndVersion() throws IOException, InterruptedException {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);
    Table table = prepareTableWithVersions(5);
    long start = table.history().get(1).snapshotId();
    long end = table.history().get(3).snapshotId();
    shell.executeStatement(String.format("create table customers2 stored by iceberg stored as %s as select * " +
            "from customers for system_version start %s end %s ORDER BY last_name", fileFormat, start, end));
    List<Object[]> rows = shell.executeStatement("SELECT * FROM customers2 ORDER BY last_name");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("Green_1", rows.get(0)[2]);
    Assert.assertEquals("Green_2", rows.get(1)[2]);
  }

  @Test
  public void testStartEndVersionWithInvalidVersion() throws IOException, InterruptedException {
    Table table = prepareTableWithVersions(5);
    long start = table.history().get(1).snapshotId();
    AssertHelpers.assertThrows("Should throw an error for non-existent snapshotID",
            IllegalArgumentException.class, "to snapshot 111222333444 does not exist",
        () -> shell.executeStatement(
                "select * from customers for system_version start " + start + " end 111222333444 ORDER BY last_name"));
  }
}
