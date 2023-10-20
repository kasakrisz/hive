package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ASTRewriteUtils {
  private ASTRewriteUtils() {}

  public static Map<String, ASTNode> collectSetColumnsAndExpressions(
      ASTNode setClause, Set<String> setRCols, Table targetTable) throws SemanticException {
    // An update needs to select all of the columns, as we rewrite the entire row.  Also,
    // we need to figure out which columns we are going to replace.
    assert setClause.getToken().getType() == HiveParser.TOK_SET_COLUMNS_CLAUSE :
        "Expected second child of update token to be set token";

    // Get the children of the set clause, each of which should be a column assignment
    List<? extends Node> assignments = setClause.getChildren();
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, ASTNode> setCols = new LinkedHashMap<String, ASTNode>(assignments.size());
    for (Node a : assignments) {
      ASTNode assignment = (ASTNode)a;
      ASTNode colName = findLHSofAssignment(assignment);
      if (setRCols != null) {
        addSetRCols((ASTNode) assignment.getChildren().get(1), setRCols);
      }
      checkValidSetClauseTarget(colName, targetTable);

      String columnName = normalizeColName(colName.getText());
      // This means that in UPDATE T SET x = _something_
      // _something_ can be whatever is supported in SELECT _something_
      setCols.put(columnName, (ASTNode)assignment.getChildren().get(1));
    }
    return setCols;
  }

  public static ASTNode findLHSofAssignment(ASTNode assignment) {
    assert assignment.getToken().getType() == HiveParser.EQUAL :
        "Expected set assignments to use equals operator but found " + assignment.getName();
    ASTNode tableOrColTok = (ASTNode)assignment.getChildren().get(0);
    assert tableOrColTok.getToken().getType() == HiveParser.TOK_TABLE_OR_COL :
        "Expected left side of assignment to be table or column";
    ASTNode colName = (ASTNode)tableOrColTok.getChildren().get(0);
    assert colName.getToken().getType() == HiveParser.Identifier :
        "Expected column name";
    return colName;
  }

  // This method finds any columns on the right side of a set statement (thus rcols) and puts them
  // in a set so we can add them to the list of input cols to check.
  private static void addSetRCols(ASTNode node, Set<String> setRCols) {

    // See if this node is a TOK_TABLE_OR_COL.  If so, find the value and put it in the list.  If
    // not, recurse on any children
    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      ASTNode colName = (ASTNode)node.getChildren().get(0);
      if (colName.getToken().getType() == HiveParser.TOK_DEFAULT_VALUE) {
        return;
      }

      assert colName.getToken().getType() == HiveParser.Identifier :
          "Expected column name";
      setRCols.add(normalizeColName(colName.getText()));
    } else if (node.getChildren() != null) {
      for (Node n : node.getChildren()) {
        addSetRCols((ASTNode)n, setRCols);
      }
    }
  }

  /**
   * Column names are stored in metastore in lower case, regardless of the CREATE TABLE statement.
   * Unfortunately there is no single place that normalizes the input query.
   * @param colName not null
   */
  private static String normalizeColName(String colName) {
    return colName.toLowerCase();
  }

  /**
   * Assert that we are not asked to update a bucketing column or partition column.
   * @param colName it's the A in "SET A = B"
   */
  public static void checkValidSetClauseTarget(ASTNode colName, Table targetTable) throws SemanticException {
    String columnName = normalizeColName(colName.getText());

    // Make sure this isn't one of the partitioning columns, that's not supported.
    for (FieldSchema fschema : targetTable.getPartCols()) {
      if (fschema.getName().equalsIgnoreCase(columnName)) {
        throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_PART_VALUE.getMsg());
      }
    }
    //updating bucket column should move row from one file to another - not supported
    if (targetTable.getBucketCols() != null && targetTable.getBucketCols().contains(columnName)) {
      throw new SemanticException(ErrorMsg.UPDATE_CANNOT_UPDATE_BUCKET_VALUE, columnName);
    }
    boolean foundColumnInTargetTable = false;
    for (FieldSchema col : targetTable.getCols()) {
      if (columnName.equalsIgnoreCase(col.getName())) {
        foundColumnInTargetTable = true;
        break;
      }
    }
    if (!foundColumnInTargetTable) {
      throw new SemanticException(ErrorMsg.INVALID_TARGET_COLUMN_IN_SET_CLAUSE, colName.getText(),
          targetTable.getFullyQualifiedName());
    }
  }

  public static void addPartitionColsAsValues(List<FieldSchema> partCols, String alias, List<String> values) {
    if (partCols == null) {
      return;
    }
    partCols.forEach(
        fieldSchema -> values.add(alias + "." + HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf)));
  }
}
