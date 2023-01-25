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

import static org.junit.Assert.*;

public class TestParseTableFor {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseVersionIntervalStart() throws Exception {
    ASTNode tree = parseDriver.parse(
            "SELECT a, b FROM t1 FOR SYSTEM_VERSION START 1234", null).getTree();

    assertTrue(tree.dump(), tree.toStringTree().contains(
            "(tok_tabname t1) (tok_open_close_version_interval 1234)))"));
  }

  @Test
  public void testParseVersionIntervalStartEnd() throws Exception {
    ASTNode tree = parseDriver.parse(
            "SELECT a, b FROM t1 FOR SYSTEM_VERSION START 1234 END 2345", null).getTree();

    assertTrue(tree.dump(), tree.toStringTree().contains(
            "(tok_tabname t1) (tok_open_close_version_interval 1234 2345)))"));
  }
}
