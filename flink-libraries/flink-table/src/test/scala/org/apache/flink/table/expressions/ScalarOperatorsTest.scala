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

package org.apache.flink.table.expressions

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.expressions.utils.{ScalarOperatorsTestBase, ShouldNotExecuteFunc}
import org.junit.Test

class ScalarOperatorsTest extends ScalarOperatorsTestBase {

  @Test
  def testCasting(): Unit = {
    // test casting
    // * -> String
    testTableApi('f2.cast(DataTypes.STRING), "f2.cast(STRING)", "1")
    testTableApi('f5.cast(DataTypes.STRING), "f5.cast(STRING)", "1.0")
    testTableApi('f3.cast(DataTypes.STRING), "f3.cast(STRING)", "1")
    testTableApi('f6.cast(DataTypes.STRING), "f6.cast(STRING)", "true")
    // NUMERIC TYPE -> Boolean
    testTableApi('f2.cast(DataTypes.BOOLEAN), "f2.cast(BOOLEAN)", "true")
    testTableApi('f7.cast(DataTypes.BOOLEAN), "f7.cast(BOOLEAN)", "false")
    testTableApi('f3.cast(DataTypes.BOOLEAN), "f3.cast(BOOLEAN)", "true")
    // NUMERIC TYPE -> NUMERIC TYPE
    testTableApi('f2.cast(DataTypes.DOUBLE), "f2.cast(DOUBLE)", "1.0")
    testTableApi('f7.cast(DataTypes.INT), "f7.cast(INT)", "0")
    testTableApi('f3.cast(DataTypes.SHORT), "f3.cast(SHORT)", "1")
    // Boolean -> NUMERIC TYPE
    testTableApi('f6.cast(DataTypes.DOUBLE), "f6.cast(DOUBLE)", "1.0")
    // identity casting
    testTableApi('f2.cast(DataTypes.INT), "f2.cast(INT)", "1")
    testTableApi('f7.cast(DataTypes.DOUBLE), "f7.cast(DOUBLE)", "0.0")
    testTableApi('f3.cast(DataTypes.LONG), "f3.cast(LONG)", "1")
    testTableApi('f6.cast(DataTypes.BOOLEAN), "f6.cast(BOOLEAN)", "true")
    // String -> BASIC TYPE (not String, Date, Void, Character)
    testTableApi('f2.cast(DataTypes.BYTE), "f2.cast(BYTE)", "1")
    testTableApi('f2.cast(DataTypes.SHORT), "f2.cast(SHORT)", "1")
    testTableApi('f2.cast(DataTypes.INT), "f2.cast(INT)", "1")
    testTableApi('f2.cast(DataTypes.LONG), "f2.cast(LONG)", "1")
    testTableApi('f3.cast(DataTypes.DOUBLE), "f3.cast(DOUBLE)", "1.0")
    testTableApi('f3.cast(DataTypes.FLOAT), "f3.cast(FLOAT)", "1.0")
    testTableApi('f5.cast(DataTypes.BOOLEAN), "f5.cast(BOOLEAN)", "true")

    // numeric auto cast in arithmetic
    testTableApi('f0 + 1, "f0 + 1", "2")
    testTableApi('f1 + 1, "f1 + 1", "2")
    testTableApi('f2 + 1L, "f2 + 1L", "2")
    testTableApi('f3 + 1.0f, "f3 + 1.0f", "2.0")
    testTableApi('f3 + 1.0d, "f3 + 1.0d", "2.0")
    testTableApi('f5 + 1, "f5 + 1", "2.0")
    testTableApi('f3 + 1.0d, "f3 + 1.0d", "2.0")
    testTableApi('f4 + 'f0, "f4 + f0", "2.0")

    // numeric auto cast in comparison
    testTableApi(
      'f0 > 0 && 'f1 > 0 && 'f2 > 0L && 'f4 > 0.0f && 'f5 > 0.0d  && 'f3 > 0,
      "f0 > 0 && f1 > 0 && f2 > 0L && f4 > 0.0f && f5 > 0.0d  && f3 > 0",
      "true")
  }

  @Test
  def testArithmetic(): Unit = {
    // math arthmetic
    testTableApi('f8 - 5, "f8 - 5", "0")
    testTableApi('f8 + 5, "f8 + 5", "10")
    testTableApi('f8 / 2, "f8 / 2", "2.5")
    testTableApi('f8 * 2, "f8 * 2", "10")
    testTableApi('f8 % 2, "f8 % 2", "1")
    testTableApi(-'f8, "-f8", "-5")
    testTableApi( +'f8, "+f8", "5") // additional space before "+" required because of checkstyle
    testTableApi(3.toExpr + 'f8, "3 + f8", "8")

    // boolean arithmetic: AND
    testTableApi('f6 && true, "f6 && true", "true")      // true && true
    testTableApi('f6 && false, "f6 && false", "false")   // true && false
    testTableApi('f11 && true, "f11 && true", "false")   // false && true
    testTableApi('f11 && false, "f11 && false", "false") // false && false
    testTableApi('f6 && 'f12, "f6 && f12", "null")       // true && null
    testTableApi('f11 && 'f12, "f11 && f12", "false")    // false && null
    testTableApi('f12 && true, "f12 && true", "null")    // null && true
    testTableApi('f12 && false, "f12 && false", "false") // null && false
    testTableApi('f12 && 'f12, "f12 && f12", "null")     // null && null
    testTableApi('f11 && ShouldNotExecuteFunc('f10),     // early out
      "f11 && ShouldNotExecuteFunc(f10)", "false")
    testTableApi('f6 && 'f11 && ShouldNotExecuteFunc('f10),  // early out
      "f6 && f11 && ShouldNotExecuteFunc(f10)", "false")

    // boolean arithmetic: OR
    testTableApi('f6 || true, "f6 || true", "true")      // true || true
    testTableApi('f6 || false, "f6 || false", "true")    // true || false
    testTableApi('f11 || true, "f11 || true", "true")    // false || true
    testTableApi('f11 || false, "f11 || false", "false") // false || false
    testTableApi('f6 || 'f12, "f6 || f12", "true")       // true || null
    testTableApi('f11 || 'f12, "f11 || f12", "null")     // false || null
    testTableApi('f12 || true, "f12 || true", "true")    // null || true
    testTableApi('f12 || false, "f12 || false", "null")  // null || false
    testTableApi('f12 || 'f12, "f12 || f12", "null")     // null || null
    testTableApi('f6 || ShouldNotExecuteFunc('f10),      // early out
      "f6 || ShouldNotExecuteFunc(f10)", "true")
    testTableApi('f11 || 'f6 || ShouldNotExecuteFunc('f10),  // early out
      "f11 || f6 || ShouldNotExecuteFunc(f10)", "true")

    // boolean arithmetic: NOT
    testTableApi(!'f6, "!f6", "false")

    // comparison
    testTableApi('f8 > 'f2, "f8 > f2", "true")
    testTableApi('f8 >= 'f8, "f8 >= f8", "true")
    testTableApi('f8 < 'f2, "f8 < f2", "false")
    testTableApi('f8.isNull, "f8.isNull", "false")
    testTableApi('f8.isNotNull, "f8.isNotNull", "true")
    testTableApi(12.toExpr <= 'f8, "12 <= f8", "false")

    // test boolean comparison
    testTableApi('f6 > 'f11, "f6 > f11", "true")
    testTableApi('f6 >= 'f11, "f6 >= f11", "true")
    testTableApi('f6 <= 'f11, "f6 <= f11", "false")
    testTableApi('f6 <= 'f12, "f6 <= f12", "null")

    // string arithmetic
    testTableApi(42.toExpr + 'f10 + 'f9, "42 + f10 + f9", "42String10")
    testTableApi('f10 + 'f9, "f10 + f9", "String10")
  }

  @Test
  def testIn(): Unit = {
    testAllApis(
      'f2.in(1, 2, 42),
      "f2.in(1, 2, 42)",
      "f2 IN (1, 2, 42)",
      "true"
    )

    testAllApis(
      'f0.in(BigDecimal(42.0), BigDecimal(2.00), BigDecimal(3.01), BigDecimal(1.000000)),
      "f0.in(42.0p, 2.00p, 3.01p, 1.000000p)",
      "CAST(f0 AS DECIMAL) IN (42.0, 2.00, 3.01, 1.000000)", // SQL would downcast otherwise
      "true"
    )

    testAllApis(
      'f10.in("This is a test String.", "String", "Hello world", "Comment#1"),
      "f10.in('This is a test String.', 'String', 'Hello world', 'Comment#1')",
      "f10 IN ('This is a test String.', 'String', 'Hello world', 'Comment#1')",
      "true"
    )

    testAllApis(
      'f14.in("This is a test String.", "Hello world"),
      "f14.in('This is a test String.', 'Hello world')",
      "f14 IN ('This is a test String.', 'String', 'Hello world')",
      "null"
    )

    testAllApis(
      'f15.in("1996-11-10".toDate),
      "f15.in('1996-11-10'.toDate)",
      "f15 IN (DATE '1996-11-10')",
      "true"
    )

    testAllApis(
      'f15.in("1996-11-10".toDate, "1996-11-11".toDate),
      "f15.in('1996-11-10'.toDate, '1996-11-11'.toDate)",
      "f15 IN (DATE '1996-11-10', DATE '1996-11-11')",
      "true"
    )

    testAllApis(
      'f7.in('f16, 'f17),
      "f7.in(f16, f17)",
      "f7 IN (f16, f17)",
      "true"
    )

    // we do not test SQL here as this expression would be converted into values + join operations
    testTableApi(
      'f7.in(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
      "f7.in(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)",
      "false"
    )

    testTableApi(
      'f10.in(
        "This is a test String.", "String", "Hello world", "Comment#1", Null(DataTypes.STRING)),
      "f10.in('This is a test String.', 'String', 'Hello world', 'Comment#1', Null(STRING))",
      "true"
    )

    testTableApi(
      'f10.in("FAIL", "FAIL"),
      "f10.in('FAIL', 'FAIL')",
      "false"
    )

    testTableApi(
      'f10.in("FAIL", "FAIL", Null(DataTypes.STRING)),
      "f10.in('FAIL', 'FAIL', Null(STRING))",
      "null"
    )
  }

  @Test
  def testOtherExpressions(): Unit = {

    // nested field null type
    testSqlApi("CASE WHEN f13.f1 IS NULL THEN 'a' ELSE 'b' END", "a")
    testSqlApi("CASE WHEN f13.f1 IS NOT NULL THEN 'a' ELSE 'b' END", "b")
    testAllApis('f13.isNull, "f13.isNull", "f13 IS NULL", "false")
    testAllApis('f13.isNotNull, "f13.isNotNull", "f13 IS NOT NULL", "true")
    testAllApis('f13.get("f0").isNull, "f13.get('f0').isNull", "f13.f0 IS NULL", "false")
    testAllApis('f13.get("f0").isNotNull, "f13.get('f0').isNotNull", "f13.f0 IS NOT NULL", "true")
    testAllApis('f13.get("f1").isNull, "f13.get('f1').isNull", "f13.f1 IS NULL", "true")
    testAllApis('f13.get("f1").isNotNull, "f13.get('f1').isNotNull", "f13.f1 IS NOT NULL", "false")

    // boolean literals
    testAllApis(
      true,
      "true",
      "true",
      "true")

    testAllApis(
      false,
      "False",
      "fAlse",
      "false")

    testAllApis(
      true,
      "TrUe",
      "tRuE",
      "true")

    // null
    testAllApis(Null(DataTypes.INT), "Null(INT)", "CAST(NULL AS INT)", "null")
    testAllApis(
      Null(DataTypes.STRING) === "",
      "Null(STRING) === ''",
      "CAST(NULL AS VARCHAR) = ''",
      "null")

    // if
    testTableApi(('f6 && true).?("true", "false"), "(f6 && true).?('true', 'false')", "true")
    testTableApi(false.?("true", "false"), "false.?('true', 'false')", "false")
    testTableApi(
      true.?(true.?(true.?(10, 4), 4), 4),
      "true.?(true.?(true.?(10, 4), 4), 4)",
      "10")
    testTableApi(true, "?((f6 && true), 'true', 'false')", "true")
    testTableApi(
      If('f9 > 'f8, 'f9 - 1, 'f9),
      "If(f9 > f8, f9 - 1, f9)",
      "9"
    )

    // case when
    testSqlApi("CASE 11 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi(
      "CASE 1 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "1 or 2")
    testSqlApi(
      "CASE 2 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
      "THEN '3' ELSE 'none of the above' END",
      "1 or 2")
    testSqlApi(
      "CASE 3 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "3")
    testSqlApi(
      "CASE 4 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "none of the above")
    testSqlApi("CASE WHEN 'a'='a' THEN 1 END", "1")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "bcd")
    testSqlApi("CASE 1 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "a")
    testSqlApi("CASE 1 WHEN 1 THEN cast('a' as varchar(1)) WHEN 2 THEN " +
      "cast('bcd' as varchar(3)) END", "a")
    testSqlApi("CASE f2 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "11")
    testSqlApi("CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "null")
    testSqlApi("CASE 42 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "null")
    testSqlApi("CASE 1 WHEN 1 THEN true WHEN 2 THEN false ELSE NULL END", "true")

    // case insensitive as
    testTableApi(5 as 'test, "5 As test", "5")

    // complex expressions
    testTableApi('f0.isNull.isNull, "f0.isNull().isNull", "false")
    testTableApi(
      'f8.abs() + 'f8.abs().abs().abs().abs(),
      "f8.abs() + f8.abs().abs().abs().abs()",
      "10")
    testTableApi(
      'f8.cast(DataTypes.STRING) + 'f8.cast(DataTypes.STRING),
      "f8.cast(STRING) + f8.cast(STRING)",
      "55")
    testTableApi('f8.isNull.cast(DataTypes.INT), "CAST(ISNULL(f8), INT)", "0")
    testTableApi(
      'f8.cast(DataTypes.INT).abs().isNull === false,
      "ISNULL(CAST(f8, INT).abs()) === false",
      "true")
    testTableApi(
      (((true === true) || false).cast(DataTypes.STRING) + "X ").trim(),
      "((((true) === true) || false).cast(STRING) + 'X ').trim",
      "trueX")
    testTableApi(12.isNull, "12.isNull", "false")
  }

  @Test
  def testBetweenExpressions(): Unit = {
    // between
    testTableApi(2.between(1, 3), "2.BETWEEN(1, 3)", "true")
    testTableApi(2.between(2, 2), "2.BETWEEN(2, 2)", "true")
    testTableApi(2.1.between(2.0, 3.0), "2.1.BETWEEN(2.0, 3.0)", "true")
    testTableApi(2.1.between(2.1, 2.1), "2.1.BETWEEN(2.1, 2.1)", "true")
    testTableApi("b".between("a", "c"), "'b'.BETWEEN('a', 'c')", "true")
    testTableApi("b".between("b", "c"), "'b'.BETWEEN('b', 'c')", "true")
    testTableApi(
      "2018-05-05".toDate.between("2018-05-01".toDate, "2018-05-10".toDate),
      "'2018-05-05'.toDate.between('2018-05-01'.toDate, '2018-05-10'.toDate)",
      "true"
    )

    // not between
    testTableApi(2.notBetween(1, 3), "2.notBetween(1, 3)", "false")
    testTableApi(2.notBetween(2, 2), "2.notBetween(2, 2)", "false")
    testTableApi(2.1.notBetween(2.0, 3.0), "2.1.notBetween(2.0, 3.0)", "false")
    testTableApi(2.1.notBetween(2.1, 2.1), "2.1.notBetween(2.1, 2.1)", "false")
    testTableApi("b".notBetween("a", "c"), "'b'.notBetween('a', 'c')", "false")
    testTableApi("b".notBetween("b", "c"), "'b'.notBetween('b', 'c')", "false")
    testTableApi(
      "2018-05-05".toDate.notBetween("2018-05-01".toDate, "2018-05-10".toDate),
      "'2018-05-05'.toDate.notBetween('2018-05-01'.toDate, '2018-05-10'.toDate)",
      "false"
    )
  }
}
