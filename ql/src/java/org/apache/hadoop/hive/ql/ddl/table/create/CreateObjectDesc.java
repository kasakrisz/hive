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

package org.apache.hadoop.hive.ql.ddl.table.create;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static org.apache.hadoop.hive.ql.ddl.DDLUtils.setColumnsAndStorePartitionTransformSpecOfTable;

/**
 * DDL task description for CREATE TABLE commands.
 */
public abstract class CreateObjectDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CreateObjectDesc.class);

  private TableName objectName;
  private List<FieldSchema> cols;
  private List<FieldSchema> partCols;
  private List<String> partColNames;
  private String comment;
  private String inputFormat;
  private String outputFormat;
  private String location;
  private String serdeName;
  private String storageHandler;
  private Map<String, String> serdeProps;
  private Map<String, String> tblProps;
  private boolean ifNotExists;
  private Long initialWriteId; // Initial write ID for CTAS and import.
  // The FSOP configuration for the FSOP that is going to write initial data during ctas.
  // This is not needed beyond compilation, so it is transient.
  private transient FileSinkDesc writer;
  private String ownerName;

  public CreateObjectDesc() {}

  public CreateObjectDesc(TableName objectName, List<FieldSchema> cols, List<FieldSchema> partCols,
                          String comment, String inputFormat,
                          String outputFormat, String location, String serdeName,
                          String storageHandler,
                          Map<String, String> serdeProps,
                          Map<String, String> tblProps,
                          boolean ifNotExists) {
    this.objectName = objectName;
    this.cols = unmodifiableList(cols);
    this.comment = comment;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.location = location;
    this.partCols = unmodifiableList(partCols);
    this.serdeName = serdeName;
    this.storageHandler = storageHandler;
    this.serdeProps = serdeProps;
    this.tblProps = tblProps;
    this.ifNotExists = ifNotExists;
  }

  private static <T> List<T> copyList(List<T> copy) {
    return copy == null ? null : new ArrayList<T>(copy);
  }

  @Explain(displayName = "columns")
  public List<String> getColsString() {
    return Utilities.getFieldSchemaString(getCols());
  }

  @Explain(displayName = "partition columns")
  public List<String> getPartColsString() {
    return Utilities.getFieldSchemaString(getPartCols());
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbTableName() {
    return objectName.getNotEmptyDbTable();
  }

  public TableName getObjectName(){ return objectName; }

  public String getDatabaseName(){
    return objectName.getDb();
  }

  public void setTableName(TableName tableName) {
    this.objectName = tableName;
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public void setPartCols(List<FieldSchema> partCols) {
    this.partCols = partCols;
  }

  public List<String> getPartColNames() {
    return partColNames;
  }

  public void setPartColNames(List<String> partColNames) {
    this.partColNames = partColNames;
  }

  @Explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Explain(displayName = "input format")
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  @Explain(displayName = "output format")
  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  @Explain(displayName = "storage handler")
  public String getStorageHandler() {
    return storageHandler;
  }

  public void setStorageHandler(String storageHandler) {
    this.storageHandler = storageHandler;
  }

  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  /**
   * @return the serDeName
   */
  @Explain(displayName = "serde name")
  public String getSerName() {
    return serdeName;
  }

  /**
   * @param serdeName
   *          the serdeName to set
   */
  public void setSerdeName(String serdeName) {
    this.serdeName = serdeName;
  }

  /**
   * @return the serDe properties
   */
  @Explain(displayName = "serde properties")
  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  /**
   * @param serdeProps
   *          the serde properties to set
   */
  public void setSerdeProps(Map<String, String> serdeProps) {
    this.serdeProps = serdeProps;
  }

  /**
   * @return the table properties
   */
  public Map<String, String> getTblProps() {
    return tblProps;
  }

  @Explain(displayName = "table properties")
  public Map<String, String> getTblPropsExplain() { // only for displaying plan
    return PlanUtils.getPropertiesForExplain(tblProps,
            hive_metastoreConstants.TABLE_IS_CTAS,
            hive_metastoreConstants.TABLE_BUCKETING_VERSION);
  }

  /**
   * @param tblProps
   *          the table properties to set
   */
  public void setTblProps(Map<String, String> tblProps) {
    this.tblProps = tblProps;
  }


  public void setInitialWriteId(Long writeId) {
    this.initialWriteId = writeId;
  }

  public Long getInitialWriteId() {
    return initialWriteId;
  }

  public FileSinkDesc getAndUnsetWriter() {
    FileSinkDesc fsd = writer;
    writer = null;
    return fsd;
  }

  public void setWriter(FileSinkDesc writer) {
    this.writer = writer;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public Table toTable(HiveConf conf) throws HiveException {

    Table tbl = new Table(objectName.getDb(), objectName.getTable());

    if (getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(getTblProps());
    }

    if (getStorageHandler() != null) {
      tbl.setProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
              getStorageHandler());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    setColumnsAndStorePartitionTransformSpecOfTable(getCols(), getPartCols(), conf, tbl);

    /*
     * If the user didn't specify a SerDe, we use the default.
     */
    String serDeClassName;
    if (serdeName == null) {
      if (storageHandler == null) {
        serDeClassName = PlanUtils.getDefaultSerDe().getName();
        LOG.info("Default to " + serDeClassName + " for table " + objectName);
      } else {
        serDeClassName = storageHandler.getSerDeClass().getName();
        LOG.info("Use StorageHandler-supplied " + serDeClassName
                + " for table " + objectName);
      }
    } else {
      // let's validate that the serde exists
      serDeClassName = serdeName;
      DDLUtils.validateSerDe(serDeClassName, conf);
    }
    tbl.setSerializationLib(serDeClassName);

    if (getSerdeProps() != null) {
      Iterator<Map.Entry<String, String>> iter = getSerdeProps().entrySet()
              .iterator();
      while (iter.hasNext()) {
        Map.Entry<String, String> m = iter.next();
        tbl.setSerdeParam(m.getKey(), m.getValue());
      }
    }

    Optional<List<FieldSchema>> cols = Optional.ofNullable(getCols());
    Optional<List<FieldSchema>> partCols = Optional.ofNullable(getPartCols());

    if (storageHandler != null && storageHandler.alwaysUnpartitioned()) {
      tbl.getSd().setCols(new ArrayList<>());
      cols.ifPresent(c -> tbl.getSd().getCols().addAll(c));
      if (partCols.isPresent() && !partCols.get().isEmpty()) {
        // Add the partition columns to the normal columns and save the transform to the session state
        tbl.getSd().getCols().addAll(partCols.get());
        List<TransformSpec> spec = PartitionTransform.getPartitionTransformSpec(partCols.get());
        if (!SessionStateUtil.addResource(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC, spec)) {
          throw new HiveException("Query state attached to Session state must be not null. " +
                  "Partition transform metadata cannot be saved.");
        }
      }
    } else {
      cols.ifPresent(c -> tbl.setFields(c));
      partCols.ifPresent(c -> tbl.setPartCols(c));
    }

    if (getComment() != null) {
      tbl.setProperty("comment", getComment());
    }
    if (getLocation() != null) {
      tbl.setDataLocation(new Path(getLocation()));
    }

    tbl.setInputFormatClass(getInputFormat());
    tbl.setOutputFormatClass(getOutputFormat());

    // only persist input/output format to metadata when it is explicitly specified.
    // Otherwise, load lazily via StorageHandler at query time.
    if (getInputFormat() != null && !getInputFormat().isEmpty()) {
      tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName());
    }
    if (getOutputFormat() != null && !getOutputFormat().isEmpty()) {
      tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName());
    }

    if (CreateTableOperation.doesTableNeedLocation(tbl)) {
      // If location is specified - ensure that it is a full qualified name
      CreateTableOperation.makeLocationQualified(tbl, conf);
    }

    if (ownerName != null) {
      tbl.setOwner(ownerName);
    }
    return tbl;
  }

}
