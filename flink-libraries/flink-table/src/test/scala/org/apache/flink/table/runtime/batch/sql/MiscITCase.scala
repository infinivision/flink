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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.junit.{Ignore, Test}

import scala.collection.Seq

/**
  * Misc tests borrowed from org.apache.spark.sql.DataFrameSuite
  */
class MiscITCase extends BatchTestBase {

  // helper methods

  private var newTableId = 0

  private val toRow = (p: Product) =>
    Row.of(p.productIterator.map(_.asInstanceOf[AnyRef]).toArray: _*)

  private def addTable[T <: Product : TypeInformation](tableData: Seq[T]): String = {
    val tableRows = tableData.map(toRow)

    val tupleTypeInfo = implicitly[TypeInformation[T]]
    val fieldInfos = tupleTypeInfo.getGenericParameters.values()
    import scala.collection.JavaConverters._
    val rowTypeInfo = new RowTypeInfo(fieldInfos.asScala.toArray: _*)

    newTableId += 1
    val tableName = "TestTableX" + newTableId

    val fields = rowTypeInfo.getFieldNames.mkString(",")

    registerCollection(tableName, tableRows, rowTypeInfo, fields)
    tableName
  }

  private def checkQuery[T <: Product : TypeInformation](
      table1Data: Seq[T],
      sqlQuery: String,
      expected: Seq[_ <: Product],
      isSorted: Boolean = false)
    : Unit = {
    val table2Data: Seq[Tuple1[String]] = null
    checkQuery2(table1Data, table2Data, sqlQuery, expected, isSorted)
  }

  private def checkQuery2[T1 <: Product : TypeInformation, T2 <: Product : TypeInformation](
      table1Data: Seq[T1],
      table2Data: Seq[T2],
      sqlQuery: String,
      expected: Seq[_ <: Product],
      isSorted: Boolean = false)
    : Unit = {

    var sqlQueryX: String = sqlQuery
    if (table1Data!=null) {
      val table1Name = addTable(table1Data)
      sqlQueryX = sqlQueryX.replace("Table1", table1Name)
    }
    if (table2Data!=null) {
      val table2Name = addTable(table2Data)
      sqlQueryX = sqlQueryX.replace("Table2", table2Name)
    }

    checkResult(sqlQueryX, expected.map(toRow), isSorted)
  }

  /// tests

  @Test
  def testBasicSelect(): Unit = {
    val testData = (1 to 100).map(i=>(i, i.toString))
    checkQuery(
      testData,
      "select * from Table1",
      testData
    )
    checkQuery(
      testData,
      "select f1 from Table1 where f0=1",
      Seq(Tuple1("1"))
    )
    checkQuery(
      testData,
      "select sum(f0), avg(f0), count(1) from Table1",
      Seq((5050, 50.5, 100L))
    )
    val testData2 = Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2))
    checkQuery(
      testData2,
      "select f0+f1, f0<f1 from Table1",
      Seq((2, false), (3, true), (3, false), (4, false), (4, false), (5, false))
    )
    checkQuery(
      testData2,
      "select sum(distinct f0) from Table1",
      Seq(Tuple1(6))
    )

  }

  @Test
  def testNullOrderingOnMultiColumn(): Unit = {
    def seq(x: (Integer, String)*): Seq[(Integer, String)] = x
    val testData = seq(Tuple2(1, "a"), Tuple2(1, null), Tuple2(null, null), Tuple2(null, "b"))
    checkQuery(
      testData,
      "select * from Table1 order by f0, f1 asc",
      seq(Tuple2(null, null), Tuple2(null, "b"), Tuple2(1, null), Tuple2(1, "a")),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 asc nulls first, f1 asc nulls first",
      seq(Tuple2(null, null), Tuple2(null, "b"), Tuple2(1, null), Tuple2(1, "a")),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 asc nulls last, f1 asc nulls first",
      seq(Tuple2(1, null), Tuple2(1, "a"), Tuple2(null, null), Tuple2(null, "b")),
      isSorted = true
    )

    checkQuery(
      testData,
      "select * from Table1 order by f0 asc nulls first, f1 asc nulls last",
      seq(Tuple2(null, "b"), Tuple2(null, null), Tuple2(1, "a"), Tuple2(1, null)),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 asc nulls last, f1 asc nulls last",
      seq(Tuple2(1, "a"), Tuple2(1, null), Tuple2(null, "b"), Tuple2(null, null)),
      isSorted = true
    )

    checkQuery(
      testData,
      "select * from Table1 order by f0 desc, f1 asc",
      seq(Tuple2(1, null), Tuple2(1, "a"), Tuple2(null, null), Tuple2(null, "b")),
      isSorted = true
    )

    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls last, f1 desc nulls last",
      seq(Tuple2(1, "a"), Tuple2(1, null), Tuple2(null, "b"), Tuple2(null, null)),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls first, f1 desc nulls last",
      seq(Tuple2(null, "b"), Tuple2(null, null), Tuple2(1, "a"), Tuple2(1, null)),
      isSorted = true
    )

    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls last, f1 desc nulls first",
      seq(Tuple2(1, null), Tuple2(1, "a"), Tuple2(null, null), Tuple2(null, "b")),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls first, f1 desc nulls first",
      seq(Tuple2(null, null), Tuple2(null, "b"), Tuple2(1, null), Tuple2(1, "a")),
      isSorted = true
    )

    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls last, f1 asc nulls last",
      seq(Tuple2(1, "a"), Tuple2(1, null), Tuple2(null, "b"), Tuple2(null, null)),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls first, f1 asc nulls last",
      seq(Tuple2(null, "b"), Tuple2(null, null), Tuple2(1, "a"), Tuple2(1, null)),
      isSorted = true
    )

    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls last, f1 asc nulls first",
      seq(Tuple2(1, null), Tuple2(1, "a"), Tuple2(null, null), Tuple2(null, "b")),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls first, f1 asc nulls first",
      seq(Tuple2(null, null), Tuple2(null, "b"), Tuple2(1, null), Tuple2(1, "a")),
      isSorted = true
    )
  }

  @Test
  def testNullOrdering(): Unit = {
    def seq(x: Integer*): Seq[Tuple1[Integer]] = x.map(Tuple1(_))
    val testData = seq(2, 1, null)
    checkQuery(
      testData,
      "select * from Table1 order by f0 asc",
      seq(null, 1, 2),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 asc nulls first",
      seq(null, 1, 2),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 asc nulls last",
      seq(1, 2, null),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc",
      seq(2, 1, null),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls last",
      seq(2, 1, null),
      isSorted = true
    )
    checkQuery(
      testData,
      "select * from Table1 order by f0 desc nulls first",
      seq(null, 2, 1),
      isSorted = true
    )
  }

  @Test
  def testGlobalSorting(): Unit = {
    val testData2 = Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2))
    checkQuery(
      testData2,
      "select * from Table1 order by f0 asc, f1 asc",
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      isSorted = true
    )
    checkQuery(
      testData2,
      "select * from Table1 order by f0 asc, f1 desc",
      Seq((1, 2), (1, 1), (2, 2), (2, 1), (3, 2), (3, 1)),
      isSorted = true
    )
    checkQuery(
      testData2,
      "select * from Table1 order by f0 desc, f1 desc",
      Seq((3, 2), (3, 1), (2, 2), (2, 1), (1, 2), (1, 1)),
      isSorted = true
    )
    checkQuery(
      testData2,
      "select * from Table1 order by f0 desc, f1 asc",
      Seq((3, 1), (3, 2), (2, 1), (2, 2), (1, 1), (1, 2)),
      isSorted = true
    )
  }

  @Test
  def testLimit(): Unit = {
    // a huge limit
    checkQuery(
      Seq((1, "a"), (2, "b")),
      "select * from Table1 limit 2147483638",
      Seq((1, "a"), (2, "b"))
    )
  }

  @Test
  def testExcept(): Unit = {
    checkQuery2(
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")),
      Seq((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")),
      "select * from Table1 except select * from Table2",
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
    )
    checkQuery2(
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")),
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")),
      "select * from Table1 except select * from Table2",
      Seq[(Integer, String)]()
    )
    // check null equality
    val nullInts = Seq[Tuple1[Integer]](Tuple1(1), Tuple1(2), Tuple1(null:Integer))
    val allNulls = Seq[Tuple1[Integer]](Tuple1(null:Integer), Tuple1(null:Integer))
    checkQuery(
      nullInts,
      "select * from Table1 except (select * from Table1 where 1=0)",
      nullInts
    )
    checkQuery(
      nullInts,
      "select * from Table1 except select * from Table1",
      Seq[Tuple1[Integer]]()
    )
    // check if values are de-duplicated
    checkQuery(
      allNulls,
      "select * from Table1 except (select * from Table1 where f0=0)",
      Seq[Tuple1[Integer]](Tuple1(null:Integer))
    )
    checkQuery(
      allNulls,
      "select * from Table1 except select * from Table1",
      Seq[Tuple1[Integer]]()
    )
    // check if values are de-duplicated
    checkQuery(
      Seq(("a", 1), ("a", 1), ("b", 1), ("a", 2)),
      "select * from Table1 except select * from Table1 where f1<0",
      Seq(("a", 1), ("b", 1), ("a", 2))
    )
    // check if the empty set on the left side works
    checkQuery(
      Seq(("a", 1), ("a", 1), ("b", 1), ("a", 2)),
      "select * from Table1 where 1=0 except select * from Table1",
      Seq[(String, Integer)]()
    )
    checkQuery2(
      Seq((1, 1), (2, 2), (3, 3)),
      Seq((2, 2), (3, 3), (4, 4)),
      "select * from Table1 except select * from Table2",
      Seq((1, 1))
    )
  }

  @Test
  def testIntersect(): Unit = {
    checkQuery(
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")),
      "select * from Table1 intersect select * from Table1",
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
    )
    checkQuery2(
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")),
      Seq((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")),
      "select * from Table1 intersect select * from Table2",
      Seq[(Integer, String)]()
    )
    // check null equality
    checkQuery(
      Seq[Tuple1[Integer]](Tuple1(1), Tuple1(2), Tuple1(null:Integer)),
      "select * from Table1 intersect select * from Table1",
      Seq[Tuple1[Integer]](Tuple1(1), Tuple1(2), Tuple1(null))
    )
    checkQuery(
      Seq[Tuple1[Integer]](Tuple1(null:Integer), Tuple1(null:Integer)),
      "select * from Table1 intersect select * from Table1",
      Seq[Tuple1[Integer]](Tuple1(null))
    )
    // check if values are de-duplicated
    checkQuery(
      Seq(("a", 1), ("a", 1), ("b", 1), ("a", 2)),
      "select * from Table1 intersect select * from Table1",
      Seq(("a", 1), ("b", 1), ("a", 2))
    )
    checkQuery2(
      Seq((1, 1), (2, 2), (3, 3)),
      Seq((2, 2), (3, 3), (4, 4)),
      "select * from Table1 intersect select * from Table2",
      Seq((2, 2), (3, 3))
    )
  }

  @Test
  def testOrderByAgg(): Unit = {
    checkQuery(
      Seq((1, 10), (1, 20), (10, 1), (10, 2)),
      "select f0, max(f1) from Table1 group by f0 order by sum(f1)",
      Seq((10, 2), (1, 20)),
      isSorted = true
    )
  }

  @Test
  def testCastInFilter(): Unit = {
    checkQuery(
      Seq((1,"a")),
      "select * from Table1 where cast(f0 as varchar(9))='1'",
      Seq((1,"a"))
    )
  }

  @Test
  def testOuterJoinWithIsNullFilter(): Unit = {
    checkQuery2(
      Seq(Tuple1("x")),
      Seq(("y", true)),
      "select * from Table1 left outer join Table2 on Table1.f0=Table2.f0",
      Seq(("x", null, null))
    )
    checkQuery2(
      Seq(Tuple1("x")),
      Seq(("y", true)),
      "select * from Table1 left outer join Table2 on Table1.f0=Table2.f0 where f1 IS NULL",
      Seq(("x", null, null))
    )
  }

  @Test
  def testNotNotNull(): Unit = {
    checkQuery(
      Seq[(Integer, Integer)]((1, 1), (2, null)),
      "select * from Table1 where NOT(f1 IS NOT NULL)",
      Seq((2, null))
    )
    checkQuery(
      Seq[(Integer, Integer)]((1, 1), (2, null)),
      "select * from Table1 where NOT(-f1 IS NOT NULL)",
      Seq((2, null))
    )
  }

  @Test
  def testOuterJoin(): Unit = {
    checkQuery2(
      Seq(("a", "a!"), ("b", "b!"), ("c", "c!")),
      Seq(("a", 1), ("b", 2)),
      "select count(*) from Table1 left outer join Table2 on Table1.f0=Table2.f0 " +
        "where Table2.f0 IS NOT NULL OR Table1.f1 not in ('a!')",
      Seq(Tuple1(3))
    )
  }

  @Ignore // todo: spark allows 123L="123"
  @Test
  def testCompareLongAndString(): Unit = {
    checkQuery(
      Seq((123L, "123"), (19157170390056973L, "19157170390056971")),
      "select f0=f1 from Table1",
      Seq(Tuple1(true), Tuple1(false))
    )
  }

  @Test
  def testOrderByOrdinal(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select 7, f0, f1 from Table1 order by 1, 2, 3",
      Seq((7, 1, 1), (7, 1, 2), (7, 2, 1), (7, 2, 2), (7, 3, 1), (7, 3, 2)),
      isSorted = true
    )
  }

  @Test // lots of when..then clauses
  def testLargeCaseWhen(): Unit = {
    // when f0=0 then 0 when f0=1 then 1 ...
    val w1 = (0 to 10).map(i=>s"when f0=$i then $i").mkString(" ")
    val w2 = (0 to 10).map(i=>s"when f0=$i then ${i + 10}").mkString(" ") + " else 0"
    checkQuery(
      Seq(Tuple1(5)),
      s"select case $w1 end, case $w2 end from Table1",
      Seq((5, 15))
    )
  }
}
