package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateRewriter implements Rewriter<UpdateSemanticAnalyzer.UpdateBlock> {

  protected final HiveConf conf;
  protected final MultiInsertSqlBuilder sqlBuilder;

  public UpdateRewriter(HiveConf conf, MultiInsertSqlBuilder sqlBuilder) {
    this.conf = conf;
    this.sqlBuilder = sqlBuilder;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateSemanticAnalyzer.UpdateBlock updateBlock)
      throws SemanticException {

    sqlBuilder.append("insert into table ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());
    sqlBuilder.addPartitionColsToInsert(updateBlock.getTargetTable().getPartCols());

    int columnOffset = sqlBuilder.getDeleteValues(Context.Operation.UPDATE).size();
    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.UPDATE);
    sqlBuilder.removeLastChar();

    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetCols().size());
    // Must be deterministic order set for consistent q-test output across Java versions

    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      sqlBuilder.append(",");
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      sqlBuilder.append(HiveUtils.unparseIdentifier(name, this.conf));
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(columnOffset + i, setCol);
      }
    }

    sqlBuilder.append(" from ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());

    // Add a sort by clause so that the row ids come out in the correct order
    sqlBuilder.appendSortBy(sqlBuilder.getSortKeys());

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode) rewrittenTree.getChildren().get(1);
    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);

    if (updateBlock.getWhereTree() != null) {
      assert rewrittenInsert.getToken().getType() == HiveParser.TOK_INSERT :
          "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName();
      // The structure of the AST for the rewritten insert statement is:
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //                        \-> TOK_SORTBY
      // Or
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //
      // The following adds the TOK_WHERE and its subtree from the original query as a child of
      // TOK_INSERT, which is where it would have landed if it had been there originally in the
      // string.  We do it this way because it's easy then turning the original AST back into a
      // string and reparsing it.
      if (rewrittenInsert.getChildren().size() == 3) {
        // We have to move the SORT_BY over one, so grab it and then push it to the second slot,
        // and put the where in the first slot
        ASTNode sortBy = (ASTNode) rewrittenInsert.getChildren().get(2);
        assert sortBy.getToken().getType() == HiveParser.TOK_SORTBY :
            "Expected TOK_SORTBY to be third child of TOK_INSERT, but found " + sortBy.getName();
        rewrittenInsert.addChild(sortBy);
        rewrittenInsert.setChild(2, updateBlock.getSetClauseTree());
      } else {
        ASTNode select = (ASTNode) rewrittenInsert.getChildren().get(1);
        assert select.getToken().getType() == HiveParser.TOK_SELECT :
            "Expected TOK_SELECT to be second child of TOK_INSERT, but found " + select.getName();
        rewrittenInsert.addChild(updateBlock.getSetClauseTree());
      }
    }

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }

  // Patch up the projection list for updates, putting back the original set expressions.
  // Walk through the projection list and replace the column names with the
  // expressions from the original update.  Under the TOK_SELECT (see above) the structure
  // looks like:
  // TOK_SELECT -> TOK_SELEXPR -> expr
  //           \-> TOK_SELEXPR -> expr ...
  protected void patchProjectionForUpdate(ASTNode insertBranch, Map<Integer, ASTNode> setColExprs) {
    ASTNode rewrittenSelect = (ASTNode) insertBranch.getChildren().get(1);
    assert rewrittenSelect.getToken().getType() == HiveParser.TOK_SELECT :
        "Expected TOK_SELECT as second child of TOK_INSERT but found " + rewrittenSelect.getName();
    for (Map.Entry<Integer, ASTNode> entry : setColExprs.entrySet()) {
      ASTNode selExpr = (ASTNode) rewrittenSelect.getChildren().get(entry.getKey());
      assert selExpr.getToken().getType() == HiveParser.TOK_SELEXPR :
          "Expected child of TOK_SELECT to be TOK_SELEXPR but was " + selExpr.getName();
      // Now, change it's child
      selExpr.setChild(0, entry.getValue());
    }
  }
}
