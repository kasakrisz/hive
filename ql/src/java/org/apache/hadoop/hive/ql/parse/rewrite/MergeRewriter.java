package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.StorageFormat;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MergeRewriter {
  protected final MultiInsertSqlBuilder sqlBuilder;

  public MergeRewriter(MultiInsertSqlBuilder sqlBuilder) {
    this.sqlBuilder = sqlBuilder;
  }

  public void handleSource(boolean hasWhenNotMatchedClause, String sourceAlias, String onClauseAsText) {
    sqlBuilder.append("FROM\n");
    sqlBuilder.append("(SELECT ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.MERGE);
    sqlBuilder.appendPartitionColsOfTarget();
    sqlBuilder.appendColsOfTargetTable();
    sqlBuilder.append(" FROM ").appendTargetTableName().append(") ");
    sqlBuilder.appendSubQueryAlias();
    sqlBuilder.append('\n');
    sqlBuilder.indent().append(hasWhenNotMatchedClause ? "RIGHT OUTER JOIN" : "INNER JOIN").append("\n");
    sqlBuilder.indent().append(sourceAlias);
    sqlBuilder.append('\n');
    sqlBuilder.indent().append("ON ").append(onClauseAsText).append('\n');
  }

  public void handleWhenNotMatchedInsert(String targetFullName, String columnListText, String hintStr,
                                         String valuesClause, String predicate, String extraPredicate) {
    sqlBuilder.append("INSERT INTO ").append(targetFullName);
    if (columnListText != null) {
      sqlBuilder.append(' ').append(columnListText);
    }

    sqlBuilder.append("    -- insert clause\n  SELECT ");
    if (hintStr != null) {
      sqlBuilder.append(hintStr);
    }

    sqlBuilder.append(valuesClause).append("\n   WHERE ").append(predicate);

    if (extraPredicate != null) {
      //we have WHEN NOT MATCHED AND <boolean expr> THEN INSERT
      sqlBuilder.append(" AND ").append(extraPredicate);
    }
    sqlBuilder.append('\n');
  }

  public void handleWhenMatchedUpdate(String targetAlias, List<String> values, String hintStr, String onClauseAsString,
                                      String extraPredicate, String deleteExtraPredicate) {
    List<String> valuesAndAcidSortKeys = new ArrayList<>(values.size() + 1);
    valuesAndAcidSortKeys.addAll(values);
    valuesAndAcidSortKeys.add(targetAlias + ".ROW__ID");

    sqlBuilder.append("    -- update clause").append("\n");
    sqlBuilder.appendInsertBranch(hintStr, valuesAndAcidSortKeys);

    addWhereClauseOfUpdate(onClauseAsString, extraPredicate, deleteExtraPredicate);

    sqlBuilder.appendSortBy(Collections.singletonList(targetAlias + ".ROW__ID "));
    sqlBuilder.append("\n");
  }

  protected void addWhereClauseOfUpdate(String onClauseAsString,
                                          String extraPredicate, String deleteExtraPredicate) {
    sqlBuilder.indent().append("WHERE ").append(onClauseAsString);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      sqlBuilder.append(" AND ").append(extraPredicate);
    }
    if (deleteExtraPredicate != null) {
      sqlBuilder.append(" AND NOT(").append(deleteExtraPredicate).append(")");
    }
  }

  public void handleWhenMatchedDelete(String hintStr, String onClauseAsString, String extraPredicate,
                                      String updateExtraPredicate) {
    sqlBuilder.appendDeleteBranch(hintStr);

    sqlBuilder.indent().append("WHERE ").append(onClauseAsString);
    if (extraPredicate != null) {
      //we have WHEN MATCHED AND <boolean expr> THEN DELETE
      sqlBuilder.append(" AND ").append(extraPredicate);
    }
    if (updateExtraPredicate != null) {
      sqlBuilder.append(" AND NOT(").append(updateExtraPredicate).append(")");
    }
    sqlBuilder.append("\n").indent();
    sqlBuilder.appendSortKeys();
  }

  public void handleCardinalityViolation(String targetAlias, String onClauseAsString, Hive db, HiveConf conf)
      throws SemanticException {
    //this is a tmp table and thus Session scoped and acid requires SQL statement to be serial in a
    // given session, i.e. the name can be fixed across all invocations
    String tableName = "merge_tmp_table";
    List<String> sortKeys = sqlBuilder.getSortKeys();
    sqlBuilder.append("INSERT INTO ").append(tableName)
        .append("\n  SELECT cardinality_violation(")
        .append(StringUtils.join(sortKeys, ","));
    sqlBuilder.appendPartColsOfTargetTable(targetAlias);

    sqlBuilder.append(")\n WHERE ").append(onClauseAsString)
        .append(" GROUP BY ").append(StringUtils.join(sortKeys, ","));

    sqlBuilder.appendPartColsOfTargetTable(targetAlias);

    sqlBuilder.append(" HAVING count(*) > 1");
    //say table T has partition p, we are generating
    //select cardinality_violation(ROW_ID, p) WHERE ... GROUP BY ROW__ID, p
    //the Group By args are passed to cardinality_violation to add the violating value to the error msg
    try {
      if (null == db.getTable(tableName, false)) {
        StorageFormat format = new StorageFormat(conf);
        format.processStorageFormat("TextFile");
        Table table = db.newTable(tableName);
        table.setSerializationLib(format.getSerde());
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("val", "int", null));
        table.setFields(fields);
        table.setDataLocation(Warehouse.getDnsPath(new Path(SessionState.get().getTempTableSpace(),
            tableName), conf));
        table.getTTable().setTemporary(true);
        table.setStoredAsSubDirectories(false);
        table.setInputFormatClass(format.getInputFormat());
        table.setOutputFormatClass(format.getOutputFormat());
        db.createTable(table, true);
      }
    } catch(HiveException | MetaException e) {
      throw new SemanticException(e.getMessage(), e);
    }
  }

  /**
   * This sets the destination name prefix for update clause.
   * @param insClauseIdx index of insert clause in the rewritten multi-insert represents the merge update clause.
   * @param rewrittenCtx the {@link Context} stores the prefixes
   * @return the number of prefixes set.
   */
  public int addDestNamePrefixOfUpdate(int insClauseIdx, Context rewrittenCtx) {
    rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.UPDATE);
    return 1;
  }

  @Override
  public String toString() {
    return sqlBuilder.toString();
  }
}
