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
package org.apache.hadoop.hive.ql.parse;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for parse WITHIN GROUP clause syntax.
 * function(expression) WITHIN GROUP (ORDER BY sort_expression)
 */
public class TestValuesClause {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseValues() throws Exception {
    ASTNode tree = parseDriver.parse(
            "VALUES(1,2,3),(4,5,6)", null).getTree();

    System.out.println(tree.dump());
  }

  @Test
  public void testParseFromValues() throws Exception {
//    ASTNode tree = parseDriver.parse("SELECT * FROM (VALUES(1,2,3),(4,5,6)) as FOO(a,b,c)", null).getTree();
    ASTNode tree = parseDriver.parse("SELECT * FROM (VALUES(1,2,3),(4,5,6)) as FOO", null).getTree();

    System.out.println(tree.dump());
  }



//    nil
//      TOK_QUERY
//        TOK_INSERT
//          TOK_DESTINATION
//            TOK_DIR
//              TOK_TMP_FILE
//          TOK_SELECT
//            TOK_SELEXPR
//                 1
//            TOK_SELEXPR
//                 2
//            TOK_SELEXPR
//                 3
//    <EOF>

  @Test
  public void testParseSelect() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select 1,2,3", null).getTree();

    System.out.println(tree.dump());
  }
}
