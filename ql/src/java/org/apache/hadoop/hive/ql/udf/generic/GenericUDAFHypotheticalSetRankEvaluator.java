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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class GenericUDAFHypotheticalSetRankEvaluator extends GenericUDAFEvaluator {

  static class RankBuffer extends AbstractAggregationBuffer {
    int rank = 0;

    @Override
    public int estimate() {
      return JavaDataModel.PRIMITIVES2;
    }
  }

  private ObjectInspector[] inputOI;

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    super.init(m, parameters);
    inputOI = parameters;
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
