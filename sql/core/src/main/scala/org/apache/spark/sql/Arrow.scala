/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector.BaseMutator
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Arrow {

  private def sparkTypeToArrowType(dataType: DataType): ArrowType = {
    dataType match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ShortType => new ArrowType.Int(8 * ShortType.defaultSize, true)
      case IntegerType => new ArrowType.Int(8 * IntegerType.defaultSize, true)
      case LongType => new ArrowType.Int(8 * LongType.defaultSize, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case ByteType => new ArrowType.Int(8, false)
      case StringType => ArrowType.Utf8.INSTANCE
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${dataType}")
    }
  }

  private def schemaToColumnWriter(allocator: RootAllocator, dataType: DataType): ColumnWriter = {
    dataType match {
      case BooleanType => new BooleanColumnWriter(allocator)
      case ShortType => new ShortColumnWriter(allocator)
      case IntegerType => new IntegerColumnWriter(allocator)
      case LongType => new LongColumnWriter(allocator)
      case FloatType => new FloatColumnWriter(allocator)
      case DoubleType => new DoubleColumnWriter(allocator)
      case ByteType => new ByteColumnWriter(allocator)
      case StringType => new UTF8StringColumnWriter(allocator)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${dataType}")
    }
  }

  /**
   * Transfer an array of InternalRow to an ArrowRecordBatch.
   */
  def internalRowsToArrowRecordBatch(
      rows: Array[InternalRow],
      schema: StructType,
      allocator: RootAllocator): ArrowRecordBatch = {
    val bufAndField = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      internalRowToArrowBuf(rows, ordinal, field, allocator)
    }

    val fieldNodes = bufAndField.flatMap(_._1).toList.asJava
    val buffers = bufAndField.flatMap(_._2).toList.asJava

    new ArrowRecordBatch(rows.length, fieldNodes, buffers)
  }

  /**
   * Convert an array of InternalRow to an ArrowBuf.
   */
  def internalRowToArrowBuf(
      rows: Array[InternalRow],
      ordinal: Int,
      field: StructField,
      allocator: RootAllocator): (Array[ArrowFieldNode], Array[ArrowBuf]) = {
    val numOfRows = rows.length
    val columnWriter = schemaToColumnWriter(allocator, field.dataType)
    columnWriter.init(numOfRows)
    var index = 0

    while(index < numOfRows) {
      val data = rows(index).get(ordinal, field.dataType)
      columnWriter.write(data)
      index += 1
    }

    val (arrowFieldNodes, arrowBufs) = columnWriter.finish()
    (arrowFieldNodes.toArray, arrowBufs.toArray)

  }

  private[sql] def schemaToArrowSchema(schema: StructType): Schema = {
    val arrowFields = schema.fields.map(sparkFieldToArrowField)
    new Schema(arrowFields.toList.asJava)
  }

  private[sql] def sparkFieldToArrowField(sparkField: StructField): Field = {
    val name = sparkField.name
    val dataType = sparkField.dataType
    val nullable = sparkField.nullable
    new Field(name, nullable, sparkTypeToArrowType(dataType), List.empty[Field].asJava)
  }
}

trait ColumnWriter {
  def init(initialSize: Int): Unit
  def write(data: Any): Unit
  def finish(): (Seq[ArrowFieldNode], Seq[ArrowBuf])
}

/**
 * Base class for flat arrow column writer, i.e., column without children.
 */
abstract class PrimitiveColumnWriter(protected val allocator: BaseAllocator) extends ColumnWriter {
  protected val valueVector: BaseDataValueVector
  protected val valueMutator: BaseMutator

  var count = 0
  var nullCount = 0

  protected def writeNull()
  protected def writeData(data: Any)
  protected def valueBuffers(): Seq[ArrowBuf]
  = valueVector.getBuffers(true) // TODO: check the flag

  override def init(initialSize: Int): Unit = {
    valueVector.allocateNew()
  }

  override def write(data: Any): Unit = {
    if (data == null) {
      writeNull()
      nullCount += 1
    } else {
      writeData(data)
    }
    count += 1
  }

  override def finish(): (Seq[ArrowFieldNode], Seq[ArrowBuf]) = {
    valueMutator.setValueCount(count)
    val fieldNode = new ArrowFieldNode(count, nullCount)
    (List(fieldNode), valueBuffers)
  }
}

class BooleanColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  implicit def bool2int(b: Boolean): Int = if (b) 1 else 0

  override protected val valueVector: NullableBitVector
  = new NullableBitVector("BooleanValue", allocator)
  override protected val valueMutator: NullableBitVector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Boolean])
}

class ShortColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableSmallIntVector
  = new NullableSmallIntVector("ShortValue", allocator)
  override protected val valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Short])
}

class IntegerColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableIntVector
  = new NullableIntVector("IntValue", allocator)
  override protected val valueMutator: NullableIntVector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Int])
}

class LongColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableBigIntVector
  = new NullableBigIntVector("LongValue", allocator)
  override protected val valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Long])
}

class FloatColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableFloat4Vector
  = new NullableFloat4Vector("FloatValue", allocator)
  override protected val valueMutator: NullableFloat4Vector#Mutator
  = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Float])
}

class DoubleColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableFloat8Vector
  = new NullableFloat8Vector("DoubleValue", allocator)
  override protected val valueMutator: NullableFloat8Vector#Mutator
  = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Double])
}

class ByteColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableUInt1Vector
  = new NullableUInt1Vector("ByteValue", allocator)
  override protected val valueMutator: NullableUInt1Vector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Byte])
}

class UTF8StringColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableVarBinaryVector
  = new NullableVarBinaryVector("UTF8StringValue", allocator)
  override protected val valueMutator: NullableVarBinaryVector#Mutator
  = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setNull(count)
  override protected def writeData(data: Any): Unit = {
    val bytes = data.asInstanceOf[UTF8String].getBytes
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}
