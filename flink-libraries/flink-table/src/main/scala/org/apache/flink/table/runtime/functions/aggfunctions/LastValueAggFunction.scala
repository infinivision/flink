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
package org.apache.flink.table.runtime.functions.aggfunctions

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType, InternalType, RowType}
import org.apache.flink.table.dataformat.{BinaryString, Decimal, GenericRow}
import org.apache.flink.table.typeutils.{BinaryStringTypeInfo, DecimalTypeInfo}

/**
  * The initial accumulator for last value aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class LastValueAggFunction[T] extends AggregateFunction[T, GenericRow] {

  def accumulate(acc: GenericRow, value: Any): Unit = {
    if (null != value) {
      acc.update(0, value.asInstanceOf[T])
    }
  }

  def accumulate(acc: GenericRow, value: Any, order: Long): Unit = {
    if (null != value && acc.getField(1).asInstanceOf[JLong] < order) {
      acc.update(0, value.asInstanceOf[T])
      acc.update(1, order)
    }
  }

  override def getValue(acc: GenericRow): T = {
    acc.getField(0).asInstanceOf[T]
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    acc.update(0, getInitValue)
    acc.update(1, JLong.MIN_VALUE)
  }

  def getInitValue: T = {
    null.asInstanceOf[T]
  }

  override def createAccumulator(): GenericRow = {
    val acc = new GenericRow(2)
    acc.update(0, getInitValue)
    acc.update(1, JLong.MIN_VALUE)
    acc
  }

  override def isDeterministic: Boolean = false

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(getInternalValueType, DataTypes.LONG)
    val fieldNames = Array("value", "time")
    new RowType(fieldTypes, fieldNames)
  }

  /**
    * DataTypes.createBaseRowType only accept InternalType, so we add the getInternalValueType
    * interface here
    */
  def getInternalValueType: InternalType

  def getValueType: DataType = getInternalValueType

  override def getResultType: DataType = getValueType

  override def getUserDefinedInputTypes(signature: Array[Class[_]]): Array[DataType] = {
    if (signature.length == 1) {
      Array[DataType](getValueType)
    } else if (signature.length == 2) {
      Array[DataType](getValueType, DataTypes.LONG)
    } else {
      throw new UnsupportedOperationException
    }
  }
}

class ByteLastValueAggFunction extends LastValueAggFunction[JByte] {
  override def getInternalValueType: InternalType = DataTypes.BYTE
}

class ShortLastValueAggFunction extends LastValueAggFunction[JShort] {
  override def getInternalValueType: InternalType = DataTypes.SHORT
}

class IntLastValueAggFunction extends LastValueAggFunction[JInt] {
  override def getInternalValueType: InternalType = DataTypes.INT
}

class LongLastValueAggFunction extends LastValueAggFunction[JLong] {
  override def getInternalValueType: InternalType = DataTypes.LONG
}

class FloatLastValueAggFunction extends LastValueAggFunction[JFloat] {
  override def getInternalValueType: InternalType = DataTypes.FLOAT
}

class DoubleLastValueAggFunction extends LastValueAggFunction[JDouble] {
  override def getInternalValueType: InternalType = DataTypes.DOUBLE
}

class BooleanLastValueAggFunction extends LastValueAggFunction[JBoolean] {
  override def getInternalValueType: InternalType = DataTypes.BOOLEAN
}

class DecimalLastValueAggFunction(decimalType: DecimalType)
  extends LastValueAggFunction[Decimal] {
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale()))
  override def getValueType: DataType =
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale())
}

class StringLastValueAggFunction extends LastValueAggFunction[BinaryString] {
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    BinaryStringTypeInfo.INSTANCE)
  override def getValueType: DataType = BinaryStringTypeInfo.INSTANCE

  override def accumulate(acc: GenericRow, value: Any): Unit = {
    if (null != value) {
      super.accumulate(acc, value.asInstanceOf[BinaryString].copy())
    }
  }

  override def accumulate(acc: GenericRow, value: Any, order: Long): Unit = {
    // just ignore nulls values and orders
    if (null != value) {
      super.accumulate(acc, value.asInstanceOf[BinaryString].copy(), order)
    }
  }
}
