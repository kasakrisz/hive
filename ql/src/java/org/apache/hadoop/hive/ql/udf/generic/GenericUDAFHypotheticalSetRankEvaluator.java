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

import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.ql.util.DirectionUtils.ASCENDING_CODE;
import static org.apache.hadoop.hive.ql.util.DirectionUtils.DESCENDING_CODE;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableLongObjectInspector;

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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFHypotheticalSetRankEvaluator extends GenericUDAFEvaluator {
  public static final String RANK_FIELD = "rank";
  public static final String COUNT_FIELD = "count";
  public static final ObjectInspector PARTIAL_RANK_OI = ObjectInspectorFactory.getStandardStructObjectInspector(
          asList(RANK_FIELD, COUNT_FIELD),
          asList(writableLongObjectInspector,
                  writableLongObjectInspector));

  protected static class HypotheticalSetRankBuffer extends AbstractAggregationBuffer {
    protected long rank = 0;
    protected long rowCount = 0;

    @Override
    public int estimate() {
      return JavaDataModel.PRIMITIVES2 * 2;
    }
  }

  protected class RankAssets {
    private final ObjectInspector commonInputOI;
    private final Converter directArgumentConverter;
    private final Converter inputConverter;
    protected final int order;
    private final NullOrdering nullOrdering;

    public RankAssets(ObjectInspector commonInputOI,
                      Converter directArgumentConverter, Converter inputConverter,
                      int order, NullOrdering nullOrdering) {
      this.commonInputOI = commonInputOI;
      this.directArgumentConverter = directArgumentConverter;
      this.inputConverter = inputConverter;
      this.order = order;
      this.nullOrdering = nullOrdering;
    }

    public int compare(Object inputValue, Object directArgumentValue) {
      return ObjectInspectorUtils.compare(inputConverter.convert(inputValue), commonInputOI,
              directArgumentConverter.convert(directArgumentValue), commonInputOI,
              new FullMapEqualComparer(), nullOrdering.getNullValueOption());
    }
  }

  public GenericUDAFHypotheticalSetRankEvaluator() {
    this(false, PARTIAL_RANK_OI, writableLongObjectInspector);
  }

  public GenericUDAFHypotheticalSetRankEvaluator(
          boolean allowEquality, ObjectInspector partialOutputOI, ObjectInspector finalOI) {
    this.allowEquality = allowEquality;
    this.partialOutputOI = partialOutputOI;
    this.finalOI = finalOI;
  }

  private final transient boolean allowEquality;
  private final transient ObjectInspector partialOutputOI;
  private final transient ObjectInspector finalOI;
  private transient List<RankAssets> rankAssetsList;
  private transient StructObjectInspector partialInputOI;
  private transient StructField partialInputRank;
  private transient StructField partialInputCount;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      rankAssetsList = new ArrayList<>(parameters.length / 4);
      for (int i = 0; i < parameters.length / 4; ++i) {
        TypeInfo directArgumentType = TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[4 * i]);
        TypeInfo inputType = TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[4 * i + 1]);
        TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForComparison(inputType, directArgumentType);
        ObjectInspector commonInputOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(commonTypeInfo);
        rankAssetsList.add(new RankAssets(
                commonInputOI,
                ObjectInspectorConverters.getConverter(parameters[4 * i], commonInputOI),
                ObjectInspectorConverters.getConverter(parameters[4 * i + 1], commonInputOI),
                ((WritableConstantIntObjectInspector) parameters[4 * i + 2]).
                        getWritableConstantValue().get(),
                NullOrdering.fromCode(((WritableConstantIntObjectInspector) parameters[4 * i + 3]).
                        getWritableConstantValue().get())));
      }
    }
    else {
      initPartial2AndFinalOI(parameters);
    }

    if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
      return partialOutputOI;
    }

    return finalOI;
  }

  protected void initPartial2AndFinalOI(ObjectInspector[] parameters) {
    partialInputOI = (StructObjectInspector) parameters[0];
    partialInputRank = partialInputOI.getStructFieldRef(RANK_FIELD);
    partialInputCount = partialInputOI.getStructFieldRef(COUNT_FIELD);
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new HypotheticalSetRankBuffer();
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
    rankBuffer.rank = 0;
    rankBuffer.rowCount = 0;
  }

  protected static class CompareResult {
    private final int compareResult;
    private final int order;

    public CompareResult(int compareResult, int order) {
      this.compareResult = compareResult;
      this.order = order;
    }

    public int getCompareResult() {
      return compareResult;
    }

    public int getOrder() {
      return order;
    }
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
    rankBuffer.rowCount++;

    CompareResult compareResult = compare(parameters);

    if (compareResult.getCompareResult() == 0) {
      if (allowEquality) {
        rankBuffer.rank++;
      }
      return;
    }

    if (compareResult.getOrder() == ASCENDING_CODE && compareResult.getCompareResult() < 0 ||
            compareResult.getOrder() == DESCENDING_CODE && compareResult.getCompareResult() > 0) {
      rankBuffer.rank++;
    }
  }

  protected CompareResult compare(Object[] parameters) {
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
      return new CompareResult(c, -1);
    }

    return new CompareResult(c, rankAssetsList.get(i).order);
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
    LongWritable[] result = new LongWritable[2];
    result[0] = new LongWritable(rankBuffer.rank + 1);
    result[1] = new LongWritable(rankBuffer.rowCount);
    return result;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    if (partial == null) {
      return;
    }

    Object objRank = partialInputOI.getStructFieldData(partial, partialInputRank);
    Object objCount = partialInputOI.getStructFieldData(partial, partialInputCount);

    HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
    rankBuffer.rank += ((LongWritable)objRank).get() - 1;
    rankBuffer.rowCount += ((LongWritable)objCount).get();
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
    return new LongWritable(rankBuffer.rank + 1);
  }
}
