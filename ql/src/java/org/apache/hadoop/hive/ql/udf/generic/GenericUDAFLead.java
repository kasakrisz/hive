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
import java.util.Deque;
import java.util.LinkedList;
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

  static class Counter {

    private final AtomicInteger numberOfValues;
    private final AtomicInteger lead;

    Counter(int leadAmt) {
      this.numberOfValues = new AtomicInteger(0);
      lead = new AtomicInteger(leadAmt);
    }

    public void incNumberOfValues() {
      numberOfValues.incrementAndGet();
    }

    public int numberOfValues() {
      return numberOfValues.get();
    }

    public void decLead() {
      lead.decrementAndGet();
    }

    public int lead() {
      return lead.get();
    }
  }

  static class Entry {
    Object value;
    long rowIdx;
  }

  static class NoNullLeadBuffer extends LeadBuffer {
    Deque<Entry> counters;
    boolean prevWasNull;
    int readerIdx;

    @Override
    public void initialize(int leadAmt) {
      super.initialize(leadAmt);
      this.counters = new LinkedList<>();
      prevWasNull = false;
      readerIdx = 0;
    }

    @Override
    public void addRow(Object leadExprValue, Object defaultValue) {
      if (leadExprValue != null) {
        Entry entry = new Entry();
        entry.rowIdx = lastRowIdx + leadAmt;
        entry.value = leadExprValue;
        counters.add(entry);
      }

//      leadWindow[nextPosInWindow] = defaultValue;
//      nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
      lastRowIdx++;
    }

    @Override
    public Object terminate() {
      int numberOfValues = 0;
      for (Counter counter : counters) {
        numberOfValues += counter.numberOfValues();
      }

      if (numberOfValues > leadAmt) {
        for (Counter counter : counters) {
          numberOfValues--;
          if (numberOfValues == 0) {
            break;
          }
          values.add(null);
        }
//        for (int i = 0; i < counters.size() - leadAmt; ++i) {
//          values.add(null);
//        }
      }
      return super.terminate();
    }
  }
//  static class NoNullLeadBuffer extends LeadBuffer {
//    List<Counter> counters;
//    boolean prevWasNull;
//
//    @Override
//    public void initialize(int leadAmt) {
//      super.initialize(leadAmt);
//      this.counters = new ArrayList<>();
//      prevWasNull = false;
//    }
//
//    @Override
//    public void addRow(Object leadExprValue, Object defaultValue) {
//      if (leadExprValue != null) {
//        counters.add(new Counter(leadAmt + 1));
//        for (Counter counter : counters) {
//          if (counter.lead() > 0) {
//            counter.decLead();
//          }
//        }
//
//        for (Counter counter : counters) {
//          if (counter.lead() == 0)
//          values.add(leadExprValue);
//        }
//        counters.removeIf(counter -> counter.lead() == 0);
//        prevWasNull = false;
//      } else {
//        if (!prevWasNull) {
//          counters.get(counters.size() - 1).incNumberOfValues();
//        } else {
//          counters.add(new Counter(leadAmt));
//        }
//        prevWasNull = true;
//      }
//
//      leadWindow[nextPosInWindow] = defaultValue;
//      nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
//      lastRowIdx++;
//    }
//
//    @Override
//    public Object terminate() {
//      int numberOfValues = 0;
//      for (Counter counter : counters) {
//        numberOfValues += counter.numberOfValues();
//      }
//
//      if (numberOfValues > leadAmt) {
//        for (Counter counter : counters) {
//          numberOfValues--;
//          if (numberOfValues == 0) {
//            break;
//          }
//          values.add(null);
//        }
////        for (int i = 0; i < counters.size() - leadAmt; ++i) {
////          values.add(null);
////        }
//      }
//      return super.terminate();
//    }
//  }

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
      NoNullLeadBuffer lb = (NoNullLeadBuffer) agg;
      if (lb.counters.size() >= lb.leadAmt) {
        Object res = lb.counters.getLast().value;
        if (res == null) {
          return ISupportStreamingModeForWindowing.NULL_RESULT;
        }
        lb.readerIdx++;
        if (lb.readerIdx >= lb.counters.getFirst().rowIdx) {
          lb.counters.removeFirst();
        }
        return res;
      }
      return null;
    }
//    @Override
//    public Object getNextResult(AggregationBuffer agg) throws HiveException {
//      LeadBuffer lb = (LeadBuffer) agg;
//      if (!lb.values.isEmpty()) {
//        Object res = lb.values.remove(0);
//        if (res == null) {
//          return ISupportStreamingModeForWindowing.NULL_RESULT;
//        }
//        return res;
//      }
//      return null;
//    }

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
