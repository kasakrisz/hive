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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

@Description(
        name = "lead",
        value = "_FUNC_(expr, amt, default)")
@WindowFunctionDescription(
        supportsWindow = false,
        pivotResult = true,
        impliesOrder = true,
        supportsNullTreatment = true)
public class GenericUDAFLead extends GenericUDAFLeadLag {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFLead.class.getName());


  @Override
  protected String functionName() {
    return "Lead";
  }

  @Override
  protected GenericUDAFLeadLagEvaluator createLLEvaluator() {
    return new GenericUDAFLeadEvaluator();
  }

  public static class GenericUDAFLeadEvaluator extends GenericUDAFLeadLagEvaluator {

    public GenericUDAFLeadEvaluator() {
    }

    /*
     * used to initialize Streaming Evaluator.
     */
    protected GenericUDAFLeadEvaluator(GenericUDAFLeadLagEvaluator src) {
      super(src);
    }

    @Override
    protected LeadLagBuffer getNewLLBuffer() {
     return respectNulls() ? new LeadBuffer() : new NoNullLeadBuffer();
    }
    
    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {

      return new GenericUDAFLeadEvaluatorStreaming(this);
    }

  }

  static class LeadBuffer implements LeadLagBuffer {
    ArrayList<Object> values;
    int leadAmt;
    Object[] leadWindow;
    int nextPosInWindow;
    int lastRowIdx;

    public void initialize(int leadAmt) {
      this.leadAmt = leadAmt;
      values = new ArrayList<Object>();
      leadWindow = new Object[leadAmt];
      nextPosInWindow = 0;
      lastRowIdx = -1;
    }

    public void addRow(Object leadExprValue, Object defaultValue) {
      int row = lastRowIdx + 1;
      int leadRow = row - leadAmt;
      if ( leadRow >= 0) {
        values.add(leadExprValue);
      }
      leadWindow[nextPosInWindow] = defaultValue;
      nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
      lastRowIdx++;
    }

    public Object terminate() {
      /*
       * if there are fewer than leadAmt values in leadWindow; start reading from the first position.
       * Otherwise the window starts from nextPosInWindow.
       */
      if ( lastRowIdx < leadAmt ) {
        nextPosInWindow = 0;
      }
      for(int i=0; i < leadAmt; i++) {
        values.add(leadWindow[nextPosInWindow]);
        nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
      }
      return values;
    }

  }

  static class NoNullLeadBuffer extends LeadBuffer {
    List<AtomicInteger> counters;

    @Override
    public void initialize(int leadAmt) {
      super.initialize(leadAmt);
      this.counters = new ArrayList<>();
    }

    @Override
    public void addRow(Object leadExprValue, Object defaultValue) {
      if (leadExprValue != null) {
        counters.add(new AtomicInteger(leadAmt + 1));
        for (AtomicInteger counter : counters) {
          if (counter.get() > 0) {
            counter.decrementAndGet();
          }
        }

        long zeros = counters.stream().filter(atomicInteger -> atomicInteger.get() == 0).count();
        for (long i = 0; i < zeros; ++i) {
          values.add(leadExprValue);
        }
        counters.removeIf(atomicInteger -> atomicInteger.get() == 0);
      } else {
        counters.add(new AtomicInteger(leadAmt));
      }

      leadWindow[nextPosInWindow] = defaultValue;
      nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
      lastRowIdx++;
    }
  }

  /*
   * StreamingEval: wrap regular eval. on getNext remove first row from values
   * and return it.
   */
  static class GenericUDAFLeadEvaluatorStreaming extends
      GenericUDAFLeadEvaluator implements ISupportStreamingModeForWindowing {

    protected GenericUDAFLeadEvaluatorStreaming(GenericUDAFLeadLagEvaluator src) {
      super(src);
    }

    @Override
    public Object getNextResult(AggregationBuffer agg) throws HiveException {
      LeadBuffer lb = (LeadBuffer) agg;
      if (!lb.values.isEmpty()) {
        Object res = lb.values.remove(0);
        if (res == null) {
          return ISupportStreamingModeForWindowing.NULL_RESULT;
        }
        return res;
      }
      return null;
    }

    @Override
    public int getRowsRemainingAfterTerminate(AggregationBuffer agg) throws HiveException {
      if (agg == null) {
        return getAmt();
      }
      LeadBuffer lb = (LeadBuffer) agg;
      return lb.values.size();
    }
  }

}
