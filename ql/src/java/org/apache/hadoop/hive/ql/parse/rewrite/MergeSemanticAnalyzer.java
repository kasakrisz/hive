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

import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTErrorUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RewriteSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.rewrite.UpdateSemanticAnalyzer.DELETE_PREFIX;

public class MergeSemanticAnalyzer extends RewriteSemanticAnalyzer2 {
  private int numWhenMatchedUpdateClauses;
  private int numWhenMatchedDeleteClauses;

  public MergeSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    return (ASTNode)tree.getChild(0);
  }

  @Override
  protected void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException {
    if (tree.getToken().getType() != HiveParser.TOK_MERGE) {
      throw new SemanticException("Asked to parse token " + tree.getName() + " in " +
          "MergeSemanticAnalyzer");
    }

    /*
     * See org.apache.hadoop.hive.ql.parse.TestMergeStatement for some examples of the merge AST
      For example, given:
      MERGE INTO acidTbl USING nonAcidPart2 source ON acidTbl.a = source.a2
      WHEN MATCHED THEN UPDATE SET b = source.b2
      WHEN NOT MATCHED THEN INSERT VALUES (source.a2, source.b2)

      We get AST like this:
      "(tok_merge " +
        "(tok_tabname acidtbl) (tok_tabref (tok_tabname nonacidpart2) source) " +
        "(= (. (tok_table_or_col acidtbl) a) (. (tok_table_or_col source) a2)) " +
        "(tok_matched " +
        "(tok_update " +
        "(tok_set_columns_clause (= (tok_table_or_col b) (. (tok_table_or_col source) b2))))) " +
        "(tok_not_matched " +
        "tok_insert " +
        "(tok_value_row (. (tok_table_or_col source) a2) (. (tok_table_or_col source) b2))))");

        And need to produce a multi-insert like this to execute:
        FROM acidTbl RIGHT OUTER JOIN nonAcidPart2 ON acidTbl.a = source.a2
        INSERT INTO TABLE acidTbl SELECT nonAcidPart2.a2, nonAcidPart2.b2 WHERE acidTbl.a IS null
        INSERT INTO TABLE acidTbl SELECT target.ROW__ID, nonAcidPart2.a2, nonAcidPart2.b2
        WHERE nonAcidPart2.a2=acidTbl.a SORT BY acidTbl.ROW__ID
    */
    /*todo: we need some sort of validation phase over original AST to make things user friendly; for example, if
     original command refers to a column that doesn't exist, this will be caught when processing the rewritten query but
     the errors will point at locations that the user can't map to anything
     - VALUES clause must have the same number of values as target table (including partition cols).  Part cols go last
     in Select clause of Insert as Select
     todo: do we care to preserve comments in original SQL?
     todo: check if identifiers are properly escaped/quoted in the generated SQL - it's currently inconsistent
      Look at UnparseTranslator.addIdentifierTranslation() - it does unescape + unparse...
     todo: consider "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET TargetTable.Col1 = SourceTable.Col1 "; what happens when
     source is empty?  This should be a runtime error - maybe not the outer side of ROJ is empty => the join produces 0
     rows. If supporting WHEN NOT MATCHED BY SOURCE, then this should be a runtime error
    */
    if (tree.getToken().getType() != HiveParser.TOK_MERGE) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
          "MergeSemanticAnalyzer");
    }

    IdentifierQuoter identifierQuoter = new IdentifierQuoter(ctx.getTokenRewriteStream());

    ctx.setOperation(Context.Operation.MERGE);
    ASTNode source = (ASTNode)tree.getChild(1);
    String targetName = getSimpleTableName(tableName);
    String sourceName = getSimpleTableName(source);
    ASTNode onClause = (ASTNode) tree.getChild(2);
    String onClauseAsText = getMatchedText(identifierQuoter, onClause);

    int whenClauseBegins = 3;
    boolean hasHint = false;
    // query hint
    ASTNode qHint = (ASTNode) tree.getChild(3);
    if (qHint.getType() == HiveParser.QUERY_HINT) {
      hasHint = true;
      whenClauseBegins++;
    }
    List<ASTNode> whenClauses = findWhenClauses(tree, whenClauseBegins);

    String subQueryAlias = isAliased(targetName) ? targetName : table.getTTable().getTableName();

    MultiInsertSqlBuilder multiInsertSqlBuilder = getSqlBuilder(subQueryAlias, DELETE_PREFIX);
    Rewriter rewriter = new MergeRewriter(multiInsertSqlBuilder);

    columnAppender.appendAcidSelectColumns(rewrittenQueryStr, Context.Operation.MERGE);

    rewrittenQueryStr.deleteCharAt(rewrittenQueryStr.length() - 1); // remove last ','
    addColsToSelect(targetTable.getCols(), rewrittenQueryStr);
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
    updateOutputs(table);

  }

  public static class MergeBlock {
    private final Table targetTable;


    public MergeBlock(Table targetTable) {
      this.targetTable = targetTable;
    }

    public Table getTargetTable() {
      return targetTable;
    }
  }

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it.
   * Since HiveLexer.g is written such that it strips away any ` (back ticks) around
   * quoted identifiers we need to add those back to generated SQL.
   * Additionally, the parser only produces tokens of type Identifier and never
   * QuotedIdentifier (HIVE-6013).  So here we just quote all identifiers.
   * (') around String literals are retained w/o issues
   */
  private static class IdentifierQuoter {
    private final TokenRewriteStream trs;
    private final IdentityHashMap<ASTNode, ASTNode> visitedNodes = new IdentityHashMap<>();

    IdentifierQuoter(TokenRewriteStream trs) {
      this.trs = trs;
      if (trs == null) {
        throw new IllegalArgumentException("Must have a TokenRewriteStream");
      }
    }

    private void visit(ASTNode n) {
      if (n.getType() == HiveParser.Identifier) {
        if (visitedNodes.containsKey(n)) {
          /**
           * Since we are modifying the stream, it's not idempotent.  Ideally, the caller would take
           * care to only quote Identifiers in each subtree once, but this makes it safe
           */
          return;
        }
        visitedNodes.put(n, n);
        trs.insertBefore(n.getToken(), "`");
        trs.insertAfter(n.getToken(), "`");
      }
      if (n.getChildCount() <= 0) {
        return;
      }
      for (Node c : n.getChildren()) {
        visit((ASTNode)c);
      }
    }
  }

  /**
   * This allows us to take an arbitrary ASTNode and turn it back into SQL that produced it without
   * needing to understand what it is (except for QuotedIdentifiers).
   */
  private String getMatchedText(IdentifierQuoter identifierQuoter, ASTNode n) {
    identifierQuoter.visit(n);
    return ctx.getTokenRewriteStream().toString(n.getTokenStartIndex(),
        n.getTokenStopIndex() + 1).trim();
  }

  /**
   * Collect WHEN clauses from Merge statement AST.
   */
  private List<ASTNode> findWhenClauses(ASTNode tree, int start) throws SemanticException {
    assert tree.getType() == HiveParser.TOK_MERGE;
    List<ASTNode> whenClauses = new ArrayList<>();
    for (int idx = start; idx < tree.getChildCount(); idx++) {
      ASTNode whenClause = (ASTNode)tree.getChild(idx);
      assert whenClause.getType() == HiveParser.TOK_MATCHED ||
          whenClause.getType() == HiveParser.TOK_NOT_MATCHED :
          "Unexpected node type found: " + whenClause.getType() + addParseInfo(whenClause);
      whenClauses.add(whenClause);
    }
    if (whenClauses.size() <= 0) {
      //Futureproofing: the parser will actually not allow this
      throw new SemanticException("Must have at least 1 WHEN clause in MERGE statement");
    }
    return whenClauses;
  }

  private static String addParseInfo(ASTNode n) {
    return " at " + ASTErrorUtils.renderPosition(n);
  }

  protected boolean isAliased(ASTNode n) {
    switch (n.getType()) {
      case HiveParser.TOK_TABREF:
        return findTabRefIdxs(n)[0] != 0;
      case HiveParser.TOK_TABNAME:
        return false;
      case HiveParser.TOK_SUBQUERY:
        assert n.getChildCount() > 1 : "Expected Derived Table to be aliased";
        return true;
      default:
        throw raiseWrongType("TOK_TABREF|TOK_TABNAME", n);
    }
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return numWhenMatchedUpdateClauses == 0 && numWhenMatchedDeleteClauses == 0;
  }
}
