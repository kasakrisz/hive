package org.apache.hadoop.hive.ql.optimizer.calcite.rules;/*
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import java.util.ArrayList;
import java.util.List;

public class HiveRewriteLeadIgnoreNullsRule extends RelOptRule {

  public static final HiveRewriteLeadIgnoreNullsRule INSTANCE = new HiveRewriteLeadIgnoreNullsRule();

  private HiveRewriteLeadIgnoreNullsRule() {
    super(operand(Project.class, any()),
            HiveRelFactories.HIVE_BUILDER,
            "HiveRewriteLeadIgnoreNullsRule");
  }


  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    HiveProject project = relOptRuleCall.rel(0);
    RexBuilder rexBuilder = relOptRuleCall.builder().getRexBuilder();
    boolean rewriteHappened = false;
    List<RexNode> newExpressions = new ArrayList<>(project.getProjects().size());
    for (RexNode projectExpr : project.getProjects()) {
      RexNode newExpr = new LeadTransformer(rexBuilder).apply(projectExpr);
      if (projectExpr != newExpr) {
        rewriteHappened = true;
      }
      newExpressions.add(newExpr);
    }

    if (!rewriteHappened) {
      return;
    }

    relOptRuleCall.transformTo(relOptRuleCall.builder().push(project.getInput()).project(newExpressions).build());
  }

  private static class LeadTransformer extends RexShuttle {
    private final RexBuilder rexBuilder;

    LeadTransformer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitOver(RexOver over) {
      if (over.ignoreNulls() && over.getAggOperator().getKind() == SqlKind.OTHER_FUNCTION &&
              over.getOperands().size() == 3 && "lead".equals(over.getAggOperator().getName())) {
        RexNode defaultValue = over.getOperands().get(2);
        if (!(defaultValue instanceof RexLiteral)) {
          defaultValue = rexBuilder.makeCast(over.getType(), defaultValue, true);

          RexNode leadNoDefault = rexBuilder.makeOver(
                  over.getType(),
                  over.getAggOperator(),
                  over.getOperands().subList(0, 2),
                  over.getWindow().partitionKeys,
                  over.getWindow().orderKeys,
                  over.getWindow().getLowerBound(),
                  over.getWindow().getUpperBound(),
                  over.getWindow().isRows(),
                  true,
                  false,
                  over.isDistinct(),
                  over.ignoreNulls());

          RexNode isNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, leadNoDefault);
          return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
                  isNull, defaultValue,
                  leadNoDefault);
        }
      }
      return super.visitOver(over);
    }
  }
}
