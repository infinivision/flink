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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

class FlinkAggregateJoinTransposeRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(RuleSets.ofList(
              AggregateReduceGroupingRule.INSTANCE
            )).build(), "reduce unless grouping")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(RuleSets.ofList(
              AggregateReduceGroupingRule.INSTANCE,
              FlinkFilterJoinRule.FILTER_ON_JOIN,
              FlinkFilterJoinRule.JOIN,
              FilterAggregateTransposeRule.INSTANCE,
              FilterProjectTransposeRule.INSTANCE,
              FilterMergeRule.INSTANCE,
              AggregateProjectMergeRule.INSTANCE,
              FlinkAggregateJoinTransposeRule.EXTENDED
            )).build(), "aggregate join transpose")
        .build()
    )
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Int, String)]("T", 'a, 'b, 'c)
    util.addTable[(Int, Int, String)]("T2", Set(Set("b2")), 'a2, 'b2, 'c2)
  }

  @Test
  def testPushCountAggThroughJoinOverUniqueColumn(): Unit = {
    val sqlQuery = "SELECT COUNT(A.a) FROM (SELECT DISTINCT a FROM T) AS A JOIN T AS B ON A.a=B.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushSumAggThroughJoinOverUniqueColumn(): Unit = {
    val sqlQuery = "SELECT SUM(A.a) FROM (SELECT DISTINCT a FROM T) AS A JOIN T AS B ON A.a=B.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushAggThroughJoinWithUniqueJoinKey(): Unit = {
    val sqlQuery =
      """
        |WITH T1 AS (SELECT a AS a1, COUNT(b) AS b1 FROM T GROUP BY a),
        |     T2 AS (SELECT COUNT(a) AS a2, b AS b2 FROM T GROUP BY b)
        |SELECT MIN(a1), MIN(b1), MIN(a2), MIN(b2), a, b, COUNT(c) FROM
        |  (SELECT * FROM T1, T2, T WHERE a1 = b2 AND a1 = a) t GROUP BY a, b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSomeAggCallColumnsAndJoinConditionColumnsIsSame(): Unit = {
    util.verifyPlan("SELECT MIN(a2), MIN(b2), a, b, COUNT(c2) FROM " +
      "(SELECT * FROM T2, T WHERE b2 = a) t GROUP BY a, b")
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsUnique1(): Unit = {
    val sqlQuery =
      """
        |select a2, b2, c2, SUM(a) FROM (
        | SELECT * FROM T2, T WHERE b2 = b
        |) GROUP BY a2, b2, c2
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsUnique2(): Unit = {
    val sqlQuery =
      """
        |select a2, b2, c, SUM(a) FROM (
        | SELECT * FROM T2, T WHERE b2 = b
        |) GROUP BY a2, b2, c
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsNotUnique1(): Unit = {
    val sqlQuery =
      """
        |select a2, b2, c2, SUM(a) FROM (
        | SELECT * FROM T2, T WHERE a2 = a
        |) GROUP BY a2, b2, c2
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithAuxGroup_JoinKeyIsNotUnique2(): Unit = {
    val sqlQuery =
      """
        |select a2, b2, c, SUM(a) FROM (
        | SELECT * FROM T2, T WHERE a2 = a
        |) GROUP BY a2, b2, c
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

}
