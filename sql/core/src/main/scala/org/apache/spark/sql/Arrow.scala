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
import org.apache.arrow.vector.{BaseDataValueVector, BitVector, IntVector, VarBinaryVector}
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
      case ShortType => new ArrowType.Int(4 * ShortType.defaultSize, true) // TODO - check on this
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
    // TODO: Implement more types
    dataType match {
      case IntegerType => new IntegerColumnWriter(allocator)
      case StringType => new UTF8StringColumnWriter(allocator)
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
  protected val validityVector = new BitVector("validity", allocator)
  protected val validityMutator = validityVector.getMutator
  protected val valueVector: BaseDataValueVector
  protected val valueMutator: BaseMutator

  var count = 0
  var nullCount = 0

  protected def writeNull()
  protected def writeData(data: Any)
  protected def valueBufs(): Seq[ArrowBuf] = List(valueVector.getBuffer)

  override def init(initialSize: Int): Unit = {
    validityVector.allocateNew(initialSize)
    valueVector.allocateNew()
  }

  override def write(data: Any): Unit = {
    if (data == null) {
      validityMutator.setSafe(count, 0)
      writeNull()
      nullCount += 1
    } else {
      validityMutator.setSafe(count, 1)
      writeData(data)
    }

    count += 1
  }

  override def finish(): (Seq[ArrowFieldNode], Seq[ArrowBuf]) = {
    validityMutator.setValueCount(count)
    valueMutator.setValueCount(count)

    val fieldNode = new ArrowFieldNode(count, nullCount)
    (List(fieldNode), validityVector.getBuffer +: valueBufs)
  }
}

class IntegerColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: IntVector = new IntVector("IntValue", allocator)
  override protected val valueMutator: IntVector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.set(count, 0)
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[Int])
}

class UTF8StringColumnWriter(allocator: BaseAllocator) extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: VarBinaryVector
  = new VarBinaryVector("UTF8StringValue", allocator)
  override protected val valueMutator: VarBinaryVector#Mutator = valueVector.getMutator

  override protected def writeNull(): Unit = valueMutator.setSafe(count, Array.empty[Byte])
  override protected def writeData(data: Any): Unit
  = valueMutator.setSafe(count, data.asInstanceOf[UTF8String].getBytes)
  override protected def valueBufs(): Seq[ArrowBuf]
  = List(valueVector.getOffsetVector.getBuffer, valueVector.getBuffer)
}
