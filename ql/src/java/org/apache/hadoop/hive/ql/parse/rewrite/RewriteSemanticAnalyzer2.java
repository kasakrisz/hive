package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class RewriteSemanticAnalyzer2 extends CalcitePlanner {
  protected static final Logger LOG = LoggerFactory.getLogger(RewriteSemanticAnalyzer2.class);

  protected static final String INDENT = "  ";
  private Table targetTable;
  private String targetTableFullName;

  public RewriteSemanticAnalyzer2(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode tree) throws SemanticException {
//    if (useSuper) {
//      super.analyzeInternal(tree);
//    } else {
//      quotedIdentifierHelper = new RewriteSemanticAnalyzer.IdentifierQuoter(ctx.getTokenRewriteStream());
      analyze(tree);
//      cleanUpMetaColumnAccessControl();
//    }
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

  protected MultiInsertSqlBuilder getColumnAppender(String subQueryAlias, String deletePrefix) {
    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(targetTable, true);
    return nonNativeAcid ? new NonNativeAcidMultiInsertSqlBuilder(targetTable, targetTableFullName, conf, subQueryAlias, deletePrefix) :
        new NativeAcidMultiInsertSqlBuilder(targetTable, targetTableFullName, conf, subQueryAlias);
  }

  static void addPartitionColsAsValues(List<FieldSchema> partCols, String alias, List<String> values, HiveConf conf) {
    if (partCols == null) {
      return;
    }
    partCols.forEach(
        fieldSchema -> values.add(alias + "." + HiveUtils.unparseIdentifier(fieldSchema.getName(), conf)));
  }
}
