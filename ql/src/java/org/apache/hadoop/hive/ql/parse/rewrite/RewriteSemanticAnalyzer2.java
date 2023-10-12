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

package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class RewriteSemanticAnalyzer2 extends CalcitePlanner {
  protected static final Logger LOG = LoggerFactory.getLogger(RewriteSemanticAnalyzer2.class);

  protected boolean useSuper = false;
  protected static final String INDENT = "  ";
  private Table targetTable;
  private String targetTableFullName;

  public RewriteSemanticAnalyzer2(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode tree) throws SemanticException {
    if (useSuper) {
      super.analyzeInternal(tree);
    } else {
//      quotedIdentifierHelper = new RewriteSemanticAnalyzer.IdentifierQuoter(ctx.getTokenRewriteStream());
      analyze(tree);
      cleanUpMetaColumnAccessControl();
    }
  }

  protected abstract ASTNode getTargetTableNode(ASTNode tree);

  private void analyze(ASTNode tree) throws SemanticException {
    ASTNode tableName = getTargetTableNode(tree);

    targetTableFullName = getFullTableNameForSQL(tableName);
    targetTable = getTable(tableName, db);
    validateTxnManager(targetTable);
    validateTargetTable(targetTable);
    analyze(tree, targetTable, tableName);
  }

  protected Table getTable(ASTNode tabRef, Hive db) throws SemanticException {
    TableName tableName;
    switch (tabRef.getType()) {
      case HiveParser.TOK_TABREF:
        tableName = getQualifiedTableName((ASTNode) tabRef.getChild(0));
        break;
      case HiveParser.TOK_TABNAME:
        tableName = getQualifiedTableName(tabRef);
        break;
      default:
        throw raiseWrongType("TOK_TABREF|TOK_TABNAME", tabRef);
    }

    Table mTable;
    try {
      mTable = db.getTable(tableName.getDb(), tableName.getTable(), tableName.getTableMetaRef(), true);
    } catch (InvalidTableException e) {
      LOG.error("Failed to find table " + tableName.getNotEmptyDbTable() + " got exception " + e.getMessage());
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName.getNotEmptyDbTable()), e);
    } catch (HiveException e) {
      LOG.error("Failed to find table " + tableName.getNotEmptyDbTable() + " got exception " + e.getMessage());
      throw new SemanticException(e.getMessage(), e);
    }
    return mTable;
  }

  private void validateTxnManager(Table mTable) throws SemanticException {
    if (!AcidUtils.acidTableWithoutTransactions(mTable) && !getTxnMgr().supportsAcid()) {
      throw new SemanticException(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getMsg());
    }
  }

  /**
   * Assert it supports Acid write.
   */
  protected void validateTargetTable(Table mTable) throws SemanticException {
    if (mTable.getTableType() == TableType.VIRTUAL_VIEW || mTable.getTableType() == TableType.MATERIALIZED_VIEW) {
      LOG.error("Table " + mTable.getFullyQualifiedName() + " is a view or materialized view");
      throw new SemanticException(ErrorMsg.UPDATE_DELETE_VIEW.getMsg());
    }
  }

  protected abstract void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException;

  protected MultiInsertSqlBuilder getSqlBuilder(String subQueryAlias, String deletePrefix) {
    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(targetTable, true);
    return nonNativeAcid ? new NonNativeAcidMultiInsertSqlBuilder(targetTable, targetTableFullName, conf, subQueryAlias, deletePrefix) :
        new NativeAcidMultiInsertSqlBuilder(targetTable, targetTableFullName, conf, subQueryAlias);
  }

  public void analyzeRewrittenTree(ASTNode rewrittenTree, Context rewrittenCtx) throws SemanticException {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Rewritten AST {}", rewrittenTree.dump());
      }
      useSuper = true;
      super.analyze(rewrittenTree, rewrittenCtx);
    } finally {
      useSuper = false;
    }
  }

  /**
   * SemanticAnalyzer will generate a WriteEntity for the target table since it doesn't know/check
   * if the read and write are of the same table in "insert ... select ....".  Since DbTxnManager
   * uses Read/WriteEntity objects to decide which locks to acquire, we get more concurrency if we
   * have changed the table WriteEntity to a set of partition WriteEntity objects based on
   * ReadEntity objects computed for this table.
   */
  protected void updateOutputs(Table targetTable) {
    markReadEntityForUpdate();

    if (targetTable.isPartitioned()) {
      List<ReadEntity> partitionsRead = getRestrictedPartitionSet(targetTable);
      if (!partitionsRead.isEmpty()) {
        // if there is WriteEntity with WriteType=UPDATE/DELETE for target table, replace it with
        // WriteEntity for each partition
        List<WriteEntity> toRemove = new ArrayList<>();
        for (WriteEntity we : outputs) {
          WriteEntity.WriteType wt = we.getWriteType();
          if (isTargetTable(we, targetTable) &&
              (wt == WriteEntity.WriteType.UPDATE || wt == WriteEntity.WriteType.DELETE)) {
            // The assumption here is that SemanticAnalyzer will will generate ReadEntity for each
            // partition that exists and is matched by the WHERE clause (which may be all of them).
            // Since we don't allow updating the value of a partition column, we know that we always
            // write the same (or fewer) partitions than we read.  Still, the write is a Dynamic
            // Partition write - see HIVE-15032.
            toRemove.add(we);
          }
        }
        outputs.removeAll(toRemove);
        // TODO: why is this like that?
        for (ReadEntity re : partitionsRead) {
          for (WriteEntity original : toRemove) {
            //since we may have both Update and Delete branches, Auth needs to know
            WriteEntity we = new WriteEntity(re.getPartition(), original.getWriteType());
            we.setDynamicPartitionWrite(original.isDynamicPartitionWrite());
            outputs.add(we);
          }
        }
      }
    }
  }

  /**
   * If the optimizer has determined that it only has to read some of the partitions of the
   * target table to satisfy the query, then we know that the write side of update/delete
   * (and update/delete parts of merge)
   * can only write (at most) that set of partitions (since we currently don't allow updating
   * partition (or bucket) columns).  So we want to replace the table level
   * WriteEntity in the outputs with WriteEntity for each of these partitions
   * ToDo: see if this should be moved to SemanticAnalyzer itself since it applies to any
   * insert which does a select against the same table.  Then SemanticAnalyzer would also
   * be able to not use DP for the Insert...
   *
   * Note that the Insert of Merge may be creating new partitions and writing to partitions
   * which were not read  (WHEN NOT MATCHED...).  WriteEntity for that should be created
   * in MoveTask (or some other task after the query is complete).
   */
  private List<ReadEntity> getRestrictedPartitionSet(Table targetTable) {
    List<ReadEntity> partitionsRead = new ArrayList<>();
    for (ReadEntity re : inputs) {
      if (re.isFromTopLevelQuery && re.getType() == Entity.Type.PARTITION && isTargetTable(re, targetTable)) {
        partitionsRead.add(re);
      }
    }
    return partitionsRead;
  }

  /**
   * Does this Entity belong to target table (partition).
   */
  private boolean isTargetTable(Entity entity, Table targetTable) {
    //todo: https://issues.apache.org/jira/browse/HIVE-15048
    // Since any DDL now advances the write id, we should ignore the write Id,
    // while comparing two tables
    return targetTable.equalsWithIgnoreWriteId(entity.getTable());
  }

  /**
   *  Walk through all our inputs and set them to note that this read is part of an update or a delete.
   */
  protected void markReadEntityForUpdate() {
    for (ReadEntity input : inputs) {
      if (isWritten(input)) {
        //TODO: this is actually not adding anything since LockComponent uses a Trie to "promote" a lock
        //except by accident - when we have a partitioned target table we have a ReadEntity and WriteEntity
        //for the table, so we mark ReadEntity and then delete WriteEntity (replace with Partition entries)
        //so DbTxnManager skips Read lock on the ReadEntity....
        input.setUpdateOrDelete(true); //input.noLockNeeded()?
      }
    }
  }

  /**
   *  For updates, we need to set the column access info so that it contains information on
   *  the columns we are updating.
   *  (But not all the columns of the target table even though the rewritten query writes
   *  all columns of target table since that is an implementation detail).
   */
  protected void setUpAccessControlInfoForUpdate(Table mTable, Map<String, ASTNode> setCols) {
    ColumnAccessInfo cai = new ColumnAccessInfo();
    for (String colName : setCols.keySet()) {
      cai.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()), colName);
    }
    setUpdateColumnAccessInfo(cai);
  }

  /**
   * We need to weed ROW__ID out of the input column info, as it doesn't make any sense to
   * require the user to have authorization on that column.
   */
  private void cleanUpMetaColumnAccessControl() {
    //we do this for Update/Delete (incl Merge) because we introduce this column into the query
    //as part of rewrite
    if (columnAccessInfo != null) {
      columnAccessInfo.stripVirtualColumn(VirtualColumn.ROWID);
    }
  }

  /**
   * Check that {@code readEntity} is also being written.
   */
  private boolean isWritten(Entity readEntity) {
    for (Entity writeEntity : outputs) {
      //make sure to compare them as Entity, i.e. that it's the same table or partition, etc
      if (writeEntity.toString().equalsIgnoreCase(readEntity.toString())) {
        return true;
      }
    }
    return false;
  }
}
