package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.ql.parse.rewrite.RewriteSemanticAnalyzer2.addPartitionColsAsValues;
import static org.apache.hadoop.hive.ql.parse.rewrite.UpdateSemanticAnalyzer.SUB_QUERY_ALIAS;

public class SplitUpdateRewriter extends UpdateRewriter {

  private final Context.Operation operation = Context.Operation.UPDATE;

  private final HiveConf conf;
  private final MultiInsertSqlBuilder sqlBuilder;

  public SplitUpdateRewriter(HiveConf conf, MultiInsertSqlBuilder sqlBuilder) {
    this.conf = conf;
    this.sqlBuilder = sqlBuilder;
  }

  public ParseUtils.ReparseResult rewrite(Context context, ASTNode tree, Table table) throws SemanticException {
    UpdateBlock updateBlock = findSetClause(tree);
    Set<String> setRCols = new LinkedHashSet<>();
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(updateBlock.getSetClauseTree(), setRCols, table);
    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetClauseTree().getChildCount());
    Map<String, String> colNameToDefaultConstraint = ConstraintsUtils.getColNameToDefaultValueMap(table);

    sqlBuilder.append("FROM\n");
    sqlBuilder.append("(SELECT ");

    sqlBuilder.appendAcidSelectColumns(operation);
    List<String> deleteValues = sqlBuilder.getDeleteValues(operation);
    int columnOffset = deleteValues.size();

    List<String> insertValues = new ArrayList<>(table.getCols().size());
    boolean first = true;

    List<FieldSchema> nonPartCols = table.getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      if (first) {
        first = false;
      } else {
        sqlBuilder.append(",");
      }

      String name = nonPartCols.get(i).getName();
      ASTNode setCol = setCols.get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);

      if (setCol != null) {
        if (setCol.getType() == HiveParser.TOK_TABLE_OR_COL &&
            setCol.getChildCount() == 1 && setCol.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          sqlBuilder.append(colNameToDefaultConstraint.get(name));
        } else {
          sqlBuilder.append(identifier);
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up. 0th is ROW_ID
          setColExprs.put(i + columnOffset, setCol);
        }
      } else {
        sqlBuilder.append(identifier);
      }
      sqlBuilder.append(" AS ");
      sqlBuilder.append(identifier);

      insertValues.add(SUB_QUERY_ALIAS + "." + identifier);
    }
    addPartitionColsAsValues(table.getPartCols(), SUB_QUERY_ALIAS, insertValues, conf);
    sqlBuilder.append(" FROM ").append(sqlBuilder.getTargetTableFullName()).append(") ");
    sqlBuilder.append(SUB_QUERY_ALIAS).append("\n");

    sqlBuilder.appendInsertBranch(null, insertValues);
    sqlBuilder.appendInsertBranch(null, deleteValues);

    List<String> sortKeys = sqlBuilder.getSortKeys();
    sqlBuilder.appendSortBy(sortKeys);

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
        rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_INSERT);

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(2, Context.DestClausePrefix.DELETE);

    if (updateBlock.getWhereTree() != null) {
      rewrittenInsert.addChild(updateBlock.getWhereTree());
    }

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);

    return rr;

//    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

//    updateOutputs(mTable);
//
//    setUpAccessControlInfoForUpdate(mTable, setCols);
//
//    // Add the setRCols to the input list
//    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
//      return;
//    }
//
//    for (String colName : setRCols) {
//      columnAccessInfo.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()), colName);
//    }
  }
}
