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

package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.List;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public abstract class MultiInsertSqlBuilder {
  protected static final String INDENT = "  ";

  protected final Table targetTable;
  protected final String targetTableFullName;
  protected final HiveConf conf;
  protected final String subQueryAlias;
  protected final StringBuilder rewrittenQueryStr;

  protected MultiInsertSqlBuilder(Table targetTable, String targetTableFullName, HiveConf conf, String subQueryAlias) {
    this.targetTable = targetTable;
    this.targetTableFullName = targetTableFullName;
    this.conf = conf;
    this.subQueryAlias = subQueryAlias;
    this.rewrittenQueryStr = new StringBuilder();
  }

  public Table getTargetTable() {
    return targetTable;
  }

  public String getTargetTableFullName() {
    return targetTableFullName;
  }

  public abstract void appendAcidSelectColumns(Context.Operation operation);

  public void appendAcidSelectColumnsForDeletedRecords(Context.Operation operation) {
    throw new UnsupportedOperationException();
  }

  public abstract List<String> getDeleteValues(Context.Operation operation);
  public abstract List<String> getSortKeys();

  protected String qualify(String columnName) {
    if (isBlank(subQueryAlias)) {
      return columnName;
    }
    return String.format("%s.%s", subQueryAlias, columnName);
  }

  public void appendInsertBranch(String hintStr, List<String> values) {
    rewrittenQueryStr.append("INSERT INTO ").append(targetTableFullName);
    appendPartitionCols(targetTable);
    rewrittenQueryStr.append("\n");

    rewrittenQueryStr.append(INDENT);
    rewrittenQueryStr.append("SELECT ");
    if (isNotBlank(hintStr)) {
      rewrittenQueryStr.append(hintStr);
    }

    rewrittenQueryStr.append(StringUtils.join(values, ","));
    rewrittenQueryStr.append("\n");
  }

  public void appendDeleteBranch(String hintStr) {
    List<String> deleteValues = getDeleteValues(Context.Operation.DELETE);
    appendInsertBranch(hintStr, deleteValues);
  }

  public void appendPartitionColsOfTarget() {
    appendPartitionCols(targetTable);
  }

  /**
   * Append list of partition columns to Insert statement. If user specified partition spec, then
   * use it to get/set the value for partition column else use dynamic partition mode with no value.
   * Static partition mode:
   * INSERT INTO T PARTITION(partCol1=val1,partCol2...) SELECT col1, ... partCol1,partCol2...
   * Dynamic partition mode:
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   */
  public void appendPartitionCols(Table table) {
    // If the table is partitioned we have to put the partition() clause in
    List<FieldSchema> partCols = table.getPartCols();
    if (partCols == null || partCols.isEmpty()) {
      return;
    }
    rewrittenQueryStr.append(" partition (");
    appendCols(partCols);
    rewrittenQueryStr.append(")");
  }

  public void appendSortBy(List<String> keys) {
    if (keys.isEmpty()) {
      return;
    }
    rewrittenQueryStr.append(INDENT).append("SORT BY ");
    rewrittenQueryStr.append(StringUtils.join(keys, ","));
    rewrittenQueryStr.append("\n");
  }

  public void appendSortKeys() {
    appendSortBy(getSortKeys());
  }

  public MultiInsertSqlBuilder append(String sqlTextFragment) {
    rewrittenQueryStr.append(sqlTextFragment);
    return this;
  }

  @Override
  public String toString() {
    return rewrittenQueryStr.toString();
  }

  public void removeLastChar() {
    rewrittenQueryStr.setLength(rewrittenQueryStr.length() - 1);
  }

  public void appendColsOfTargetTable() {
    appendCols(targetTable.getCols());
  }

  public void appendPartColsOfTargetTable(String alias) {
    appendCols(targetTable.getPartCols(), alias);
  }

  public void appendCols(List<FieldSchema> columns) {
    appendCols(columns, null);
  }

  public void appendCols(List<FieldSchema> columns, String alias) {
    if (columns == null) {
      return;
    }

    String quotedAlias = null;
    if (isNotBlank(alias)) {
      quotedAlias = HiveUtils.unparseIdentifier(alias, this.conf);
    }

    boolean first = true;
    for (FieldSchema fschema : columns) {
      if (first) {
        first = false;
      } else {
        rewrittenQueryStr.append(", ");
      }

      if (quotedAlias != null) {
        rewrittenQueryStr.append(quotedAlias).append('.');
      }
      rewrittenQueryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
    }
  }

  public MultiInsertSqlBuilder appendTargetTableName() {
    rewrittenQueryStr.append(targetTableFullName);
    return this;
  }

  public MultiInsertSqlBuilder append(char c) {
    rewrittenQueryStr.append(c);
    return this;
  }

  public MultiInsertSqlBuilder indent() {
    rewrittenQueryStr.append(INDENT);
    return this;
  }

  public MultiInsertSqlBuilder appendSubQueryAlias() {
    rewrittenQueryStr.append(subQueryAlias);
    return this;
  }
}
