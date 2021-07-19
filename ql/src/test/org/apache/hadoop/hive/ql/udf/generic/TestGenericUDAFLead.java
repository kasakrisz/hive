package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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
class TestGenericUDAFLead {

  GenericUDAFLead.GenericUDAFLeadEvaluatorStreaming evaluator;

  @BeforeEach
  void setUp() throws HiveException {
    GenericUDAFLead.GenericUDAFLeadEvaluator baseEvaluator = new GenericUDAFLead.GenericUDAFLeadEvaluator();
    ObjectInspector[] inputIO = new ObjectInspector[] {
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.intTypeInfo)
    };
    baseEvaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, inputIO);
    baseEvaluator.setRespectNulls(true);

    evaluator = new GenericUDAFLead.GenericUDAFLeadEvaluatorStreaming(baseEvaluator);
  }

  @Test
  void testRespectNulls() throws HiveException {
    GenericUDAFLead.LeadBuffer buffer = leadBuffer();

    evaluator.iterate(buffer, parameters(8));
    assertThat(evaluator.getNextResult(buffer), is(nullValue()));

    evaluator.iterate(buffer, parameters(null));
    assertThat(evaluator.getNextResult(buffer), is(nullValue()));

    evaluator.iterate(buffer, parameters(null));
    assertThat(evaluator.getNextResult(buffer), is(ISupportStreamingModeForWindowing.NULL_RESULT));

    evaluator.iterate(buffer, parameters(5));
    assertThat(evaluator.getNextResult(buffer), is(new IntWritable(5)));

    evaluator.iterate(buffer, parameters(4));
    assertThat(evaluator.getNextResult(buffer), is(new IntWritable(4)));

    evaluator.terminate(buffer);

    assertThat(evaluator.getRowsRemainingAfterTerminate(buffer), is(2));

    assertThat(evaluator.getNextResult(buffer), is(ISupportStreamingModeForWindowing.NULL_RESULT));
    assertThat(evaluator.getNextResult(buffer), is(ISupportStreamingModeForWindowing.NULL_RESULT));
  }

  private Object[] parameters(Object value) {
    Object[] parameters = new Object[1];
    parameters[0] = value == null ? null : new IntWritable((Integer) value);
    return parameters;
  }

  private GenericUDAFLead.LeadBuffer leadBuffer() {
    GenericUDAFLead.LeadBuffer buffer = new GenericUDAFLead.LeadBuffer();
    buffer.initialize(2);
    return buffer;
  }
}