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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamTestData, TestingAppendSink}
import org.apache.flink.table.util.TableTestBase
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.junit.Test

class TableSinkValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'id, 'num, 'text)

    t.groupBy('text)
    .select('text, 'id.count, 'num.sum)
    .toAppendStream[Row].addSink(new TestingAppendSink)

    // must fail because table is not append-only
    env.execute()
  }
}
