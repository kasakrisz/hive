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
package org.apache.hadoop.hive.ql.udf.generic;

import static org.apache.hadoop.hive.ql.util.DirectionUtils.ASCENDING_CODE;
import static org.apache.hadoop.hive.ql.util.DirectionUtils.DESCENDING_CODE;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

public class GenericUDAFHypotheticalSetRankEvaluator extends GenericUDAFEvaluator {

  private static class RankBuffer extends AbstractAggregationBuffer {
    int rank = 0;

    @Override
    public int estimate() {
      return JavaDataModel.PRIMITIVES2;
    }
  }

  private class RankAssets {
    private final ObjectInspector commonInputIO;
    private final Converter directArgumentConverter;
    private final Converter inputConverter;
    private final int order;
    private final NullOrdering nullOrdering;

    public RankAssets(ObjectInspector commonInputIO,
                      Converter directArgumentConverter, Converter inputConverter,
                      int order, NullOrdering nullOrdering) {
      this.commonInputIO = commonInputIO;
      this.directArgumentConverter = directArgumentConverter;
      this.inputConverter = inputConverter;
      this.order = order;
      this.nullOrdering = nullOrdering;
    }

    public int compare(Object inputValue, Object directArgumentValue) {
      return ObjectInspectorUtils.compare(inputConverter.convert(inputValue), commonInputIO,
              directArgumentConverter.convert(directArgumentValue), commonInputIO,
              new FullMapEqualComparer(), nullOrdering.getNullValueOption());
    }
  }

  private transient List<RankAssets> rankAssetsList;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      rankAssetsList = new ArrayList<>(parameters.length / 4);
      for (int i = 0; i < parameters.length / 4; ++i) {
        TypeInfo directArgumentType = TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[4 * i]);
        TypeInfo inputType = TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[4 * i + 1]);
        TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForComparison(inputType, directArgumentType);
        ObjectInspector commonInputIO = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(commonTypeInfo);
        rankAssetsList.add(new RankAssets(
                commonInputIO,
                ObjectInspectorConverters.getConverter(parameters[4 * i], commonInputIO),
                ObjectInspectorConverters.getConverter(parameters[4 * i + 1], commonInputIO),
                ((WritableConstantIntObjectInspector) parameters[4 * i + 2]).
                        getWritableConstantValue().get(),
                NullOrdering.fromCode(((WritableConstantIntObjectInspector) parameters[4 * i + 3]).
                        getWritableConstantValue().get())));
      }
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new RankBuffer();
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    RankBuffer rankBuffer = (RankBuffer) agg;
    rankBuffer.rank = 0;
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    RankBuffer rankBuffer = (RankBuffer) agg;

    int i = 0;
    int c = 0;
    for (RankAssets rankAssets : rankAssetsList) {
      c = rankAssets.compare(parameters[4 * i + 1], parameters[4 * i]);
      if (c != 0) {
        break;
      }
      ++i;
    }

    if (c == 0) {
      return;
    }

    int order = rankAssetsList.get(i).order;
    if (order == ASCENDING_CODE && c < 0 || order == DESCENDING_CODE && c > 0) {
      rankBuffer.rank++;
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    RankBuffer rankBuffer = (RankBuffer) agg;
    return new IntWritable(rankBuffer.rank + 1);
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    if (partial == null) {
      return;
    }

    IntWritable rank = (IntWritable) partial;
    RankBuffer rankBuffer = (RankBuffer) agg;
    rankBuffer.rank += rank.get() - 1;
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    RankBuffer rankBuffer = (RankBuffer) agg;
    return new IntWritable(rankBuffer.rank + 1);
  }
}
