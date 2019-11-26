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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;

public class CommonKeyPrefix {

  public static CommonKeyPrefix map(TopNKeyDesc topNKeyDesc, GroupByDesc groupByDesc) {
    return map(topNKeyDesc.getKeyColumns(), topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder(),
            groupByDesc.getKeys(), groupByDesc.getColumnExprMap(),
            topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder());
  }

  public static CommonKeyPrefix map(TopNKeyDesc topNKeyDesc, ReduceSinkDesc reduceSinkDesc) {
    return map(topNKeyDesc.getKeyColumns(), topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder(),
            reduceSinkDesc.getKeyCols(), reduceSinkDesc.getColumnExprMap(),
            reduceSinkDesc.getOrder(), reduceSinkDesc.getNullOrder());
  }

  public static CommonKeyPrefix map(
          List<ExprNodeDesc> opKeys, String opOrder, String opNullOrder,
          List<ExprNodeDesc> parentKeys,
          String parentOrder, String parentNullOrder) {

    CommonKeyPrefix commonPrefix = new CommonKeyPrefix();
    int size = Stream.of(opKeys.size(), opOrder.length(), opNullOrder.length(),
            parentKeys.size(), parentOrder.length(), parentNullOrder.length())
            .min(Integer::compareTo)
            .orElse(0);

    for (int i = 0; i < size; ++i) {
      ExprNodeDesc opKey = opKeys.get(i);
      ExprNodeDesc parentKey = parentKeys.get(i);
      if (opKey.isSame(parentKey) &&
              opOrder.charAt(i) == parentOrder.charAt(i) &&
              opNullOrder.charAt(i) == parentNullOrder.charAt(i)) {
        commonPrefix.add(parentKey, opOrder.charAt(i), opNullOrder.charAt(i));
      } else {
        return commonPrefix;
      }
    }
    return commonPrefix;
  }

  public static CommonKeyPrefix map(
          List<ExprNodeDesc> opKeys, String opOrder, String opNullOrder,
          List<ExprNodeDesc> parentKeys, Map<String, ExprNodeDesc> parentColExprMap,
          String parentOrder, String parentNullOrder) {

    if (parentColExprMap == null) {
      return map(opKeys, opOrder, opNullOrder, parentKeys, parentOrder, parentNullOrder);
    }

    CommonKeyPrefix commonPrefix = new CommonKeyPrefix();
    int size = Stream.of(opKeys.size(), opOrder.length(), opNullOrder.length(),
            parentKeys.size(), parentColExprMap.size(), parentOrder.length(), parentNullOrder.length())
            .min(Integer::compareTo)
            .orElse(0);

    for (int i = 0; i < size; ++i) {
      ExprNodeDesc column = opKeys.get(i);
      ExprNodeDesc parentKey = parentKeys.get(i);
      String columnName = column.getExprString();
      if (Objects.equals(parentColExprMap.get(columnName), parentKey) &&
              opOrder.charAt(i) == parentOrder.charAt(i) &&
              opNullOrder.charAt(i) == parentNullOrder.charAt(i)) {
        commonPrefix.add(parentKey, opOrder.charAt(i), opNullOrder.charAt(i));
      } else {
        return commonPrefix;
      }
    }
    return commonPrefix;
  }

  private List<ExprNodeDesc> mappedColumns = new ArrayList<>();
  private StringBuilder mappedOrder = new StringBuilder();
  private StringBuilder mappedNullOrder = new StringBuilder();

  public void add(ExprNodeDesc column, char order, char nullOrder) {
    mappedColumns.add(column);
    mappedOrder.append(order);
    mappedNullOrder.append(nullOrder);
  }

  public boolean isEmpty() {
    return mappedColumns.isEmpty();
  }

  public List<ExprNodeDesc> getMappedColumns() {
    return mappedColumns;
  }

  public String getMappedOrder() {
    return mappedOrder.toString();
  }

  public String getMappedNullOrder() {
    return mappedNullOrder.toString();
  }

  public int size() {
    return mappedColumns.size();
  }
}
