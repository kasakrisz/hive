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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateObjectDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.ddl.DDLUtils.setColumnsAndStorePartitionTransformSpecOfTable;

/**
 * DDL task description for CREATE VIEW commands.
 */
@Explain(displayName = "Create Materialized View", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateMaterializedViewDesc extends CreateObjectDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private String originalText;
  private String expandedText;
  private boolean rewriteEnabled;
  private Set<TableName> tablesUsed;
  private List<String> sortColNames;
  private List<FieldSchema> sortCols;
  private List<String> distributeColNames;
  private List<FieldSchema> distributeCols;

  /**
   * Used to create a materialized view descriptor.
   */
  public CreateMaterializedViewDesc(TableName viewName, List<FieldSchema> schema, String comment,
      Map<String, String> tblProps, List<String> partColNames, List<String> sortColNames,
      List<String> distributeColNames, boolean ifNotExists, boolean rewriteEnabled,
      String inputFormat, String outputFormat, String location,
      String serde, String storageHandler, Map<String, String> serdeProps) {
    super(viewName, schema, null, comment, inputFormat, outputFormat, location, serde, storageHandler,
            serdeProps, tblProps, ifNotExists);
    this.sortColNames = sortColNames;
    this.distributeColNames = distributeColNames;
    this.rewriteEnabled = rewriteEnabled;
    this.setPartColNames(partColNames);
  }

  @Explain(displayName = "original text", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getViewOriginalText() {
    return originalText;
  }

  public void setViewOriginalText(String originalText) {
    this.originalText = originalText;
  }

  @Explain(displayName = "expanded text")
  public String getViewExpandedText() {
    return expandedText;
  }

  public void setViewExpandedText(String expandedText) {
    this.expandedText = expandedText;
  }

  @Explain(displayName = "rewrite enabled", displayOnlyOnTrue = true)
  public boolean isRewriteEnabled() {
    return rewriteEnabled;
  }

  public void setRewriteEnabled(boolean rewriteEnabled) {
    this.rewriteEnabled = rewriteEnabled;
  }

  public boolean isOrganized() {
    return (sortColNames != null && !sortColNames.isEmpty()) ||
        (distributeColNames != null && !distributeColNames.isEmpty());
  }

  @Explain(displayName = "sort columns")
  public List<String> getSortColsString() {
    return Utilities.getFieldSchemaString(sortCols);
  }

  public List<FieldSchema> getSortCols() {
    return sortCols;
  }

  public void setSortCols(List<FieldSchema> sortCols) {
    this.sortCols = sortCols;
  }

  public List<String> getSortColNames() {
    return sortColNames;
  }

  public void setSortColNames(List<String> sortColNames) {
    this.sortColNames = sortColNames;
  }

  @Explain(displayName = "distribute columns")
  public List<String> getDistributeColsString() {
    return Utilities.getFieldSchemaString(distributeCols);
  }

  public List<FieldSchema> getDistributeCols() {
    return distributeCols;
  }

  public void setDistributeCols(List<FieldSchema> distributeCols) {
    this.distributeCols = distributeCols;
  }

  public List<String> getDistributeColNames() {
    return distributeColNames;
  }

  public void setDistributeColNames(List<String> distributeColNames) {
    this.distributeColNames = distributeColNames;
  }

  public Set<TableName> getTablesUsed() {
    return tablesUsed;
  }

  public void setTablesUsed(Set<TableName> tablesUsed) {
    this.tablesUsed = tablesUsed;
  }

  public Table toTable(HiveConf conf) throws HiveException {
    Table tbl = super.toTable(conf);
    tbl.setViewOriginalText(getViewOriginalText());
    tbl.setViewExpandedText(getViewExpandedText());
    tbl.setRewriteEnabled(isRewriteEnabled());
    tbl.setTableType(TableType.MATERIALIZED_VIEW);

    if (!CollectionUtils.isEmpty(sortColNames)) {
      tbl.setProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS,
          Utilities.encodeColumnNames(sortColNames));
    }
    if (!CollectionUtils.isEmpty(distributeColNames)) {
      tbl.setProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS,
          Utilities.encodeColumnNames(distributeColNames));
    }

    // Sets the column state for the create view statement (false since it is a creation).
    // Similar to logic in CreateTableDesc.
    StatsSetupConst.setStatsStateForCreateTable(tbl.getTTable().getParameters(), null,
        StatsSetupConst.FALSE);

    return tbl;
  }
}
