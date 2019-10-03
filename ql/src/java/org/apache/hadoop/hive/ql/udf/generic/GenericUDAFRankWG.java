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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@WindowFunctionDescription(
        description = @Description(
                name = "rankwg",
                value = "_FUNC_(x)"),
        supportsWindow = false,
        rankingFunction = true,
        supportsWithinGroup = true)
public class GenericUDAFRankWG extends AbstractGenericUDAFResolver {
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length < 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
              "One or more arguments are expected.");
    }
    for (int i = 0; i < parameters.length; i++) {
      ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[i]);
      if (!ObjectInspectorUtils.compareSupported(oi)) {
        throw new UDFArgumentTypeException(i,
                "Cannot support comparison of map<> type or complex type containing map<>.");
      }
    }
    return new GenericUDAFRankEvaluator();
  }

  public static class RankBuffer implements AggregationBuffer {
    int rank = 0;
  }

  public static class GenericUDAFRankEvaluator extends GenericUDAFEvaluator {

    ObjectInspector[] inputOI;
    ObjectInspector[] outputOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      inputOI = parameters;
      outputOI = new ObjectInspector[inputOI.length];
      for (int i = 0; i < inputOI.length; i++) {
        outputOI[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOI[i],
                ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
      }
//      return ObjectInspectorFactory.getStandardListObjectInspector(
//              PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new RankBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      int i = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      RankBuffer rb = (RankBuffer) agg;
      // TODO: order by , NULLS first/last
      if (parameters[0] == null)
        return;

      int c = ObjectInspectorUtils.compare(parameters[0], inputOI[0], parameters[1], inputOI[1]);
      if (c < 0) {
        rb.rank++;
      }
    }

    /*
     * Called when the value in the partition has changed. Update the currentRank
     */
    protected void nextRank(GenericUDAFRank.RankBuffer rb) {
      rb.currentRank = rb.currentRowNum;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      RankBuffer rb = (RankBuffer) agg;
      return new IntWritable(rb.rank + 1);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }

      IntWritable rank = (IntWritable) partial;
      RankBuffer rb = (RankBuffer) agg;
      rb.rank += rank.get() - 1;
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      RankBuffer rb = (RankBuffer) agg;
      return new IntWritable(rb.rank + 1);
    }

  }

}
