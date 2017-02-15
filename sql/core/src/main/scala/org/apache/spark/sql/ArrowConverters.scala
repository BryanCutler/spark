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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.{SeekableByteChannel, Channels}

import scala.collection.JavaConverters._

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector.BaseMutator
import org.apache.arrow.vector.file.{ArrowReader, ArrowWriter}
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._


//////////////////////
private[sql] class ByteArrayReadableSeekableByteChannel(var byteArray: Array[Byte]) extends SeekableByteChannel {
  var _position: Long = 0L

  override def isOpen: Boolean = {
    byteArray != null
  }

  override def close(): Unit = {
    byteArray = null
  }

  override def read(dst: ByteBuffer): Int = {
    val remainingBuf = byteArray.length - _position
    val length = Math.min(dst.remaining(), remainingBuf).toInt
    dst.put(byteArray, _position.toInt, length)
    _position += length
    length.toInt
  }

  override def position(): Long = _position

  override def position(newPosition: Long): SeekableByteChannel = {
    _position = newPosition.toLong
    this
  }

  override def size: Long = {
    byteArray.length.toLong
  }

  override def write(src: ByteBuffer): Int = {
    throw new UnsupportedOperationException("Read Only")
  }

  override def truncate(size: Long): SeekableByteChannel = {
    throw new UnsupportedOperationException("Read Only")
  }
}
//////////////////////

/**
 * Intermediate data structure returned from Arrow conversions
 */
private[sql] abstract class ArrowPayload extends Iterator[ArrowRecordBatch]

private[sql] class ArrowStaticPayload(batches: ArrowRecordBatch*) extends ArrowPayload {
  private val iter = batches.iterator

  override def next(): ArrowRecordBatch = iter.next()
  override def hasNext: Boolean = iter.hasNext
}

/**
 * Class that wraps an Arrow RootAllocator used in conversion
 */
private[sql] class ArrowConverters {
  private val _allocator = new RootAllocator(Long.MaxValue)

  private[sql] def allocator: RootAllocator = _allocator

  def interalRowIterToPayload(rowIter: Iterator[InternalRow], schema: StructType): ArrowPayload = {
    val batch = ArrowConverters.internalRowIterToArrowBatch(rowIter, schema, allocator)
    new ArrowStaticPayload(batch)
  }

  private[sql] def readBatchBytes(batchBytes: Array[Byte]): Array[ArrowRecordBatch] = {
    val in = new ByteArrayReadableSeekableByteChannel(batchBytes)
    val reader = new ArrowReader(in, _allocator)
    val footer = reader.readFooter()
    val batchBlocks = footer.getRecordBatches.asScala.toArray
    batchBlocks.map(block => reader.readRecordBatch(block))
  }
}

private[sql] object ArrowConverters {

  /**
   * Map a Spark Dataset type to ArrowType.
   */
  private[sql] def sparkTypeToArrowType(dataType: DataType): ArrowType = {
    dataType match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ShortType => new ArrowType.Int(8 * ShortType.defaultSize, true)
      case IntegerType => new ArrowType.Int(8 * IntegerType.defaultSize, true)
      case LongType => new ArrowType.Int(8 * LongType.defaultSize, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case ByteType => new ArrowType.Int(8, true)
      case StringType => ArrowType.Utf8.INSTANCE
      case BinaryType => ArrowType.Binary.INSTANCE
      case DateType => ArrowType.Date.INSTANCE
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MILLISECOND)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }

  /**
   * Iterate over InternalRows and write to an ArrowRecordBatch.
   */
  private def internalRowIterToArrowBatch(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      allocator: RootAllocator): ArrowRecordBatch = {

    val columnWriters = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      ColumnWriter(ordinal, allocator, field.dataType)
        .init()
    }

    val writerLength = columnWriters.length
    while (rowIter.hasNext) {
      val row = rowIter.next()
      var i = 0
      while (i < writerLength) {
        columnWriters(i).write(row)
        i += 1
      }
    }

    val fieldAndBuf = columnWriters.map { writer =>
      writer.finish()
    }.unzip
    val fieldNodes = fieldAndBuf._1
    val buffers = fieldAndBuf._2.flatten

    val rowLength = if(fieldNodes.nonEmpty) fieldNodes.head.getLength else 0

    val recordBatch = new ArrowRecordBatch(rowLength,
      fieldNodes.toList.asJava, buffers.toList.asJava)

    buffers.foreach(_.release())
    recordBatch
  }

  /**
   * Convert a Spark Dataset schema to Arrow schema.
   */
  private[sql] def schemaToArrowSchema(schema: StructType): Schema = {
    val arrowFields = schema.fields.map { f =>
      new Field(f.name, f.nullable, sparkTypeToArrowType(f.dataType), List.empty[Field].asJava)
    }
    new Schema(arrowFields.toList.asJava)
  }

  /**
   * Write an ArrowPayload to a byte array
   */
  private[sql] def payloadToByteArray(payload: ArrowPayload, schema: StructType): Array[Byte] = {
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowWriter(Channels.newChannel(out), arrowSchema)
    try {
      payload.foreach(writer.writeRecordBatch)
    } catch {
      case e: Exception =>
        throw e
    } finally {
      writer.close()
      payload.foreach(_.close())
    }
    out.toByteArray
  }
}

private[sql] trait ColumnWriter {
  def init(): this.type
  def write(row: InternalRow): Unit

  /**
   * Clear the column writer and return the ArrowFieldNode and ArrowBuf.
   * This should be called only once after all the data is written.
   */
  def finish(): (ArrowFieldNode, Array[ArrowBuf])
}

/**
 * Base class for flat arrow column writer, i.e., column without children.
 */
private[sql] abstract class PrimitiveColumnWriter(
  val ordinal: Int,
  protected val allocator: BaseAllocator)
    extends ColumnWriter {
  protected def valueVector: BaseDataValueVector
  protected def valueMutator: BaseMutator

  protected def setNull(): Unit
  protected def setValue(row: InternalRow, ordinal: Int): Unit

  protected var count = 0
  protected var nullCount = 0

  override def init(): this.type = {
    valueVector.allocateNew()
    this
  }

  override def write(row: InternalRow): Unit = {
    if (row.isNullAt(ordinal)) {
      setNull()
      nullCount += 1
    } else {
      setValue(row, ordinal)
    }
    count += 1
  }

  override def finish(): (ArrowFieldNode, Array[ArrowBuf]) = {
    valueMutator.setValueCount(count)
    val fieldNode = new ArrowFieldNode(count, nullCount)
    val valueBuffers = valueVector.getBuffers(true)
    (fieldNode, valueBuffers)
  }
}

private[sql] class BooleanColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  private def bool2int(b: Boolean): Int = if (b) 1 else 0

  override protected val valueVector: NullableBitVector
    = new NullableBitVector("BooleanValue", allocator)
  override protected val valueMutator: NullableBitVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, bool2int(row.getBoolean(ordinal)))
}

private[sql] class ShortColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableSmallIntVector
    = new NullableSmallIntVector("ShortValue", allocator)
  override protected val valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getShort(ordinal))
}

private[sql] class IntegerColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableIntVector
    = new NullableIntVector("IntValue", allocator)
  override protected val valueMutator: NullableIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getInt(ordinal))
}

private[sql] class LongColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableBigIntVector
    = new NullableBigIntVector("LongValue", allocator)
  override protected val valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getLong(ordinal))
}

private[sql] class FloatColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableFloat4Vector
    = new NullableFloat4Vector("FloatValue", allocator)
  override protected val valueMutator: NullableFloat4Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getFloat(ordinal))
}

private[sql] class DoubleColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableFloat8Vector
    = new NullableFloat8Vector("DoubleValue", allocator)
  override protected val valueMutator: NullableFloat8Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getDouble(ordinal))
}

private[sql] class ByteColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableUInt1Vector
    = new NullableUInt1Vector("ByteValue", allocator)
  override protected val valueMutator: NullableUInt1Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getByte(ordinal))
}

private[sql] class UTF8StringColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("UTF8StringValue", allocator)
  override protected val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit = {
    val bytes = row.getUTF8String(ordinal).getBytes
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class BinaryColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("BinaryValue", allocator)
  override protected val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit = {
    val bytes = row.getBinary(ordinal)
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class DateColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableDateVector
    = new NullableDateVector("DateValue", allocator)
  override protected val valueMutator: NullableDateVector#Mutator = valueVector.getMutator

  override protected def setNull(): Unit = valueMutator.setNull(count)
  override protected def setValue(row: InternalRow, ordinal: Int): Unit = {
    // TODO: comment on diff btw value representations of date/timestamp
    valueMutator.setSafe(count, row.getInt(ordinal).toLong * 24 * 3600 * 1000)
  }
}

private[sql] class TimeStampColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override protected val valueVector: NullableTimeStampMicroVector
    = new NullableTimeStampMicroVector("TimeStampValue", allocator)
  override protected val valueMutator: NullableTimeStampMicroVector#Mutator = valueVector.getMutator

  override protected def setNull(): Unit = valueMutator.setNull(count)

  override protected def setValue(row: InternalRow, ordinal: Int): Unit = {
    valueMutator.setSafe(count, row.getLong(ordinal) / 1000)
  }
}

private[sql] object ColumnWriter {
  def apply(ordinal: Int, allocator: BaseAllocator, dataType: DataType): ColumnWriter = {
    dataType match {
      case BooleanType => new BooleanColumnWriter(ordinal, allocator)
      case ShortType => new ShortColumnWriter(ordinal, allocator)
      case IntegerType => new IntegerColumnWriter(ordinal, allocator)
      case LongType => new LongColumnWriter(ordinal, allocator)
      case FloatType => new FloatColumnWriter(ordinal, allocator)
      case DoubleType => new DoubleColumnWriter(ordinal, allocator)
      case ByteType => new ByteColumnWriter(ordinal, allocator)
      case StringType => new UTF8StringColumnWriter(ordinal, allocator)
      case BinaryType => new BinaryColumnWriter(ordinal, allocator)
      case DateType => new DateColumnWriter(ordinal, allocator)
      case TimestampType => new TimeStampColumnWriter(ordinal, allocator)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}
