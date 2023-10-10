package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;

public class UpdateRewriter implements Rewriter<UpdateSemanticAnalyzer.UpdateBlock> {
  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateSemanticAnalyzer.UpdateBlock updateBlock)
      throws SemanticException {
    return null;
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
