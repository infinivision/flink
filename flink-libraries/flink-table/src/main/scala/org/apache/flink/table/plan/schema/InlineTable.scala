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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.types.{RowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic

abstract class InlineTable(
    val dataType: DataType,
    val fieldIndexes: Array[Int],
    val fieldNames: Array[String],
    val statistic: FlinkStatistic)
  extends FlinkTable {

  if (fieldIndexes.length != fieldNames.length) {
    throw new TableException(
      s"Number of field names and field indexes must be equal.\n" +
        s"Number of names is ${fieldNames.length}, number of indexes is ${fieldIndexes.length}.\n" +
        s"List of column names: ${fieldNames.mkString("[", ", ", "]")}.\n" +
        s"List of column indexes: ${fieldIndexes.mkString("[", ", ", "]")}.")
  }

  // check uniqueness of field names
  if (fieldNames.length != fieldNames.toSet.size) {
    val duplicateFields = fieldNames
      // count occurrences of field names
      .groupBy(identity).mapValues(_.length)
      // filter for occurrences > 1 and map to field name
      .filter(g => g._2 > 1).keys

    throw new TableException(
      s"Field names must be unique.\n" +
        s"List of duplicate fields: ${duplicateFields.mkString("[", ", ", "]")}.\n" +
        s"List of all fields: ${fieldNames.mkString("[", ", ", "]")}.")
  }

  val fieldTypes: Array[InternalType] =
    dataType.toInternalType match {

      case ct: RowType =>
        // it is ok to leave out fields
        if (fieldIndexes.count(_ >= 0) > ct.getArity) {
          throw new TableException(
          s"Arity of type (" + ct.getFieldNames.deep + ") " +
            "must not be greater than number of field names " + fieldNames.deep + ".")
        }
        fieldIndexes.map {
          case DataTypes.ROWTIME_STREAM_MARKER =>
            DataTypes.ROWTIME_INDICATOR
          case DataTypes.PROCTIME_STREAM_MARKER =>
            DataTypes.PROCTIME_INDICATOR
          case DataTypes.ROWTIME_BATCH_MARKER =>
            DataTypes.TIMESTAMP
          case DataTypes.PROCTIME_BATCH_MARKER =>
            DataTypes.TIMESTAMP
          case i => ct.getInternalTypeAt(i).toInternalType
        }

      case t: InternalType =>
        var cnt = 0
        val types = fieldIndexes.map {
          case DataTypes.ROWTIME_STREAM_MARKER =>
            DataTypes.ROWTIME_INDICATOR
          case DataTypes.PROCTIME_STREAM_MARKER =>
            DataTypes.PROCTIME_INDICATOR
          case DataTypes.ROWTIME_BATCH_MARKER =>
            DataTypes.TIMESTAMP
          case DataTypes.PROCTIME_BATCH_MARKER =>
            DataTypes.TIMESTAMP
          case _ =>
            cnt += 1
            t
        }

        // ensure that the atomic type is matched at most once.
        if (cnt > 1) {
          throw new TableException(
            "Non-composite input type may have only a single field and its index must be 0.")
        } else {
          types
        }
    }

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    flinkTypeFactory.buildRelDataType(fieldNames, fieldTypes)
  }

  /**
    * Returns statistics of current table
    *
    * @return statistics of current table
    */
  override def getStatistic: FlinkStatistic = statistic

   /**
    * Creates a copy of this table, changing statistic.
    *
    * @param statistic A new FlinkStatistic.
    * @return Copy of this table, substituting statistic.
    */
  def copy(statistic: FlinkStatistic): FlinkTable
}
