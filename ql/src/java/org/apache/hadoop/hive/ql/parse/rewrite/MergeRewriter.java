package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RewriteSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class MergeRewriter implements Rewriter<MergeSemanticAnalyzer.MergeBlock> {

  protected final MultiInsertSqlBuilder sqlBuilder;

  public MergeRewriter(MultiInsertSqlBuilder sqlBuilder) {
    this.sqlBuilder = sqlBuilder;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, MergeSemanticAnalyzer.MergeBlock rewriteData)
      throws SemanticException {

    sqlBuilder.append("(SELECT ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.MERGE);

    sqlBuilder.removeLastChar();
    sqlBuilder.addColsToSelect(targetTable.getCols(), rewrittenQueryStr);
    addColsToSelect(targetTable.getPartCols(), rewrittenQueryStr);
    rewrittenQueryStr.append(" FROM ").append(getFullTableNameForSQL(targetNameNode)).append(") ");
    rewrittenQueryStr.append(subQueryAlias);
    rewrittenQueryStr.append('\n');

    rewrittenQueryStr.append(INDENT).append(chooseJoinType(whenClauses)).append("\n");
    if (source.getType() == HiveParser.TOK_SUBQUERY) {
      //this includes the mandatory alias
      rewrittenQueryStr.append(INDENT).append(getMatchedText(source));
    } else {
      rewrittenQueryStr.append(INDENT).append(getFullTableNameForSQL(source));
      if (isAliased(source)) {
        rewrittenQueryStr.append(" ").append(sourceName);
      }
    }
    rewrittenQueryStr.append('\n');
    rewrittenQueryStr.append(INDENT).append("ON ").append(onClauseAsText).append('\n');

    // Add the hint if any
    String hintStr = null;
    if (hasHint) {
      hintStr = " /*+ " + qHint.getText() + " */ ";
    }
    /**
     * We allow at most 2 WHEN MATCHED clause, in which case 1 must be Update the other Delete
     * If we have both update and delete, the 1st one (in SQL code) must have "AND <extra predicate>"
     * so that the 2nd can ensure not to process the same rows.
     * Update and Delete may be in any order.  (Insert is always last)
     */
    String extraPredicate = null;
    int numInsertClauses = 0;
    numWhenMatchedUpdateClauses = 0;
    numWhenMatchedDeleteClauses = 0;
    boolean hintProcessed = false;
    for (ASTNode whenClause : whenClauses) {
      switch (getWhenClauseOperation(whenClause).getType()) {
        case HiveParser.TOK_INSERT:
          numInsertClauses++;
          handleInsert(whenClause, rewrittenQueryStr, targetNameNode, onClause,
              targetTable, targetName, onClauseAsText, hintProcessed ? null : hintStr);
          hintProcessed = true;
          break;
        case HiveParser.TOK_UPDATE:
          numWhenMatchedUpdateClauses++;
          String s = handleUpdate(whenClause, rewrittenQueryStr, targetNameNode,
              onClauseAsText, targetTable, extraPredicate, hintProcessed ? null : hintStr, columnAppender);
          hintProcessed = true;
          if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
            extraPredicate = s; //i.e. it's the 1st WHEN MATCHED
          }
          break;
        case HiveParser.TOK_DELETE:
          numWhenMatchedDeleteClauses++;
          String s1 = handleDelete(whenClause, rewrittenQueryStr,
              onClauseAsText, extraPredicate, hintProcessed ? null : hintStr, columnAppender);
          hintProcessed = true;
          if (numWhenMatchedUpdateClauses + numWhenMatchedDeleteClauses == 1) {
            extraPredicate = s1; //i.e. it's the 1st WHEN MATCHED
          }
          break;
        default:
          throw new IllegalStateException("Unexpected WHEN clause type: " + whenClause.getType() +
              addParseInfo(whenClause));
      }
      if (numWhenMatchedDeleteClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_DELETE, ctx.getCmd());
      }
      if (numWhenMatchedUpdateClauses > 1) {
        throw new SemanticException(ErrorMsg.MERGE_TOO_MANY_UPDATE, ctx.getCmd());
      }
      assert numInsertClauses < 2: "too many Insert clauses";
    }
    if (numWhenMatchedDeleteClauses + numWhenMatchedUpdateClauses == 2 && extraPredicate == null) {
      throw new SemanticException(ErrorMsg.MERGE_PREDIACTE_REQUIRED, ctx.getCmd());
    }

    boolean validating = handleCardinalityViolation(rewrittenQueryStr, targetNameNode, onClauseAsText, targetTable,
        numWhenMatchedDeleteClauses == 0 && numWhenMatchedUpdateClauses == 0, columnAppender);
    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, rewrittenQueryStr);
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;
    rewrittenCtx.setOperation(Context.Operation.MERGE);

    //set dest name mapping on new context; 1st child is TOK_FROM
    int insClauseIdx = 1;
    for (int whenClauseIdx = 0;
         insClauseIdx < rewrittenTree.getChildCount() - (validating ? 1 : 0/*skip cardinality violation clause*/);
         whenClauseIdx++) {
      //we've added Insert clauses in order or WHEN items in whenClauses
      switch (getWhenClauseOperation(whenClauses.get(whenClauseIdx)).getType()) {
        case HiveParser.TOK_INSERT:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
          ++insClauseIdx;
          break;
        case HiveParser.TOK_UPDATE:
          insClauseIdx += addDestNamePrefixOfUpdate(insClauseIdx, rewrittenCtx);
          break;
        case HiveParser.TOK_DELETE:
          rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.DELETE);
          ++insClauseIdx;
          break;
        default:
          assert false;
      }
    }
    if (validating) {
      //here means the last branch of the multi-insert is Cardinality Validation
      rewrittenCtx.addDestNamePrefix(rewrittenTree.getChildCount() - 1, Context.DestClausePrefix.INSERT);
    }

    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);
    updateOutputs(targetTable);

    return null;
  }
}
