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

import java.io.{ByteArrayOutputStream, InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}

import com.google.flatbuffers.FlatBufferBuilder
import org.apache.arrow.vector.stream.{ArrowStreamReader, ArrowStreamWriter, MessageSerializer}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import scala.collection.JavaConverters._

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector.BaseMutator
import org.apache.arrow.vector.file._
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{FieldType, ArrowType, Field, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * ArrowReader requires a seekable byte channel.
 * TODO: This is available in arrow-vector now with ARROW-615, to be included in 0.2.1 release
 */
private[sql] class ByteArrayReadableSeekableByteChannel(var byteArray: Array[Byte])
  extends SeekableByteChannel {
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
    length
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

private[sql] class ArrowPayload(initialBatch: ArrowRecordBatch) extends Serializable {

  def this() = this(null)

  var _batch = initialBatch

  def batch: ArrowRecordBatch = _batch

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val rootAllocator = new RootAllocator(Long.MaxValue)
    _batch = MessageSerializer.deserializeMessageBatch(
      new ReadChannel(Channels.newChannel(in)), rootAllocator).asInstanceOf[ArrowRecordBatch]
    //rootAllocator.close()
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch)
  }
}

/**
 * Intermediate data structure returned from Arrow conversions
 */
//private[sql] abstract class ArrowPayload extends Iterator[ArrowRecordBatchInternal]

/**
 * Build a payload from existing ArrowRecordBatches
 */
/*private[sql] class ArrowStaticPayload(batches: ArrowRecordBatch*) extends ArrowPayload {
  private val iter = batches.iterator
  override def next(): ArrowRecordBatch = iter.next()
  override def hasNext: Boolean = iter.hasNext
}*/

/**
 * Class that wraps an Arrow RootAllocator used in conversion
 */
private[sql] class ArrowConverters {
  private val _allocator = new RootAllocator(Long.MaxValue)

  private[sql] def allocator: RootAllocator = _allocator

  def interalRowIterToPayload(rowIter: Iterator[InternalRow], schema: StructType): ArrowPayload = {
    val batch = ArrowConverters.internalRowIterToArrowBatch(rowIter, schema, allocator)
    new ArrowPayload(batch)
  }

  def readPayloadByteArrays(payloadByteArrays: Array[Array[Byte]]): Iterator[ArrowPayload] = {
    // TODO: change to real iterator, to get one payload at a time
    val batches = scala.collection.mutable.ArrayBuffer.empty[ArrowPayload]
    var i = 0
    while (i < payloadByteArrays.length) {
      val payloadBytes = payloadByteArrays(i)
      val in = new ByteArrayReadableSeekableByteChannel(payloadBytes)
      val reader = new ArrowFileReader(in, _allocator)
      val root = reader.getVectorSchemaRoot
      val unloader = new VectorUnloader(root)
      reader.loadNextBatch()
      batches += new ArrowPayload(unloader.getRecordBatch)
      i += 1
    }
    batches.toIterator
  }

  def writePayloads(payloadIter: Iterator[ArrowPayload],
                                 schema: StructType,
                                 out: OutputStream): Unit = {

    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, _allocator)
    val loader = new VectorLoader(root)
    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))

    while (payloadIter.hasNext) {
      // TODO: catch exceptions
      val payload = payloadIter.next()
      loader.load(payload.batch)
      writer.writeBatch()
      payload.batch.close()
    }

    writer.end()
    // NOTE: also closes OutputStream
    // _writer.close()
  }

  def readPayloads(in: InputStream): Iterator[ArrowPayload] = {
    new Iterator[ArrowPayload] {
      private val _reader = new ArrowStreamReader(Channels.newChannel(in), allocator)
      private val _root = _reader.getVectorSchemaRoot
      private var _nextPayload = read()

      override def hasNext: Boolean = _nextPayload != null

      override def next(): ArrowPayload = {
        val obj = _nextPayload
        if (hasNext) {
          _nextPayload = read()
        }
        obj
      }

      private def read(): ArrowPayload = {
        _reader.loadNextBatch()
        if (_root.getRowCount() > 0) {
          val unloader = new VectorUnloader(_root)
          new ArrowPayload(unloader.getRecordBatch)
        } else {
          null
        }
      }
    }
  }

  def payloadToInternalRowIter(payload: ArrowPayload, schema: StructType): Iterator[InternalRow] = {
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val loader = new VectorLoader(root)
    loader.load(payload.batch)

    new Iterator[InternalRow] {
      var index = 0

      override def hasNext: Boolean = index < root.getRowCount

      override def next(): InternalRow = {
        val fields = root.getFieldVectors.asScala
        val genericRowData = fields.map { field =>
          val obj: Any = field.getAccessor.getObject(index)
          obj
        }.toArray
        index += 1
        new GenericInternalRow(genericRowData)
      }
    }
  }

  /**
   * Write an ArrowPayload to a byte array
   */
  private[sql] def payloadToByteArray(payload: ArrowPayload, schema: StructType): Array[Byte] = {
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, _allocator)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))

    // Iterate over payload batches to write each one, ensure all batches get closed
    Utils.tryWithSafeFinally {
      val loader = new VectorLoader(root)
      loader.load(payload.batch)
      writer.writeBatch()
    } {
      payload.batch.close()
      writer.close()
    }
    out.toByteArray
  }

  def close(): Unit = {
    //_allocator.close()
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
      case DateType => new ArrowType.Date(DateUnit.MILLISECOND)
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
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

    val (fieldNodes, bufferArrays) = columnWriters.map(_.finish()).unzip
    val buffers = bufferArrays.flatten

    val rowLength = if (fieldNodes.nonEmpty) fieldNodes.head.getLength else 0
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
  val allocator: BaseAllocator)
    extends ColumnWriter {
  def valueVector: BaseDataValueVector
  def valueMutator: BaseMutator

  def setNull(): Unit
  def setValue(row: InternalRow): Unit

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
      setValue(row)
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

  override val valueVector: NullableBitVector
    = new NullableBitVector("BooleanValue", FieldType.nullable(ArrowType.Bool.INSTANCE), allocator)
  override val valueMutator: NullableBitVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, bool2int(row.getBoolean(ordinal)))
}

private[sql] class ShortColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableSmallIntVector
    = new NullableSmallIntVector("ShortValue", FieldType.nullable(new ArrowType.Int(8 * ShortType.defaultSize, true)), allocator)
  override val valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getShort(ordinal))
}

private[sql] class IntegerColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableIntVector
    = new NullableIntVector("IntValue", FieldType.nullable(new ArrowType.Int(8 * IntegerType.defaultSize, true)), allocator)
  override val valueMutator: NullableIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getInt(ordinal))
}

private[sql] class LongColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableBigIntVector
    = new NullableBigIntVector("LongValue", FieldType.nullable(new ArrowType.Int(8 * LongType.defaultSize, true)), allocator)
  override val valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getLong(ordinal))
}

private[sql] class FloatColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableFloat4Vector
    = new NullableFloat4Vector("FloatValue", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), allocator)
  override val valueMutator: NullableFloat4Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getFloat(ordinal))
}

private[sql] class DoubleColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableFloat8Vector
    = new NullableFloat8Vector("DoubleValue", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), allocator)
  override val valueMutator: NullableFloat8Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getDouble(ordinal))
}

private[sql] class ByteColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableUInt1Vector
    = new NullableUInt1Vector("ByteValue", FieldType.nullable(new ArrowType.Int(8, true)), allocator)
  override val valueMutator: NullableUInt1Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getByte(ordinal))
}

private[sql] class UTF8StringColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("UTF8StringValue", FieldType.nullable(ArrowType.Utf8.INSTANCE), allocator)
  override val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    val bytes = row.getUTF8String(ordinal).getBytes
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class BinaryColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("BinaryValue", FieldType.nullable(ArrowType.Binary.INSTANCE), allocator)
  override val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    val bytes = row.getBinary(ordinal)
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class DateColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableDateVector
    = new NullableDateVector("DateValue", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), allocator)
  override val valueMutator: NullableDateVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    // TODO: comment on diff btw value representations of date/timestamp
    valueMutator.setSafe(count, row.getInt(ordinal).toLong * 24 * 3600 * 1000)
  }
}

private[sql] class TimeStampColumnWriter(ordinal: Int, allocator: BaseAllocator)
    extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableTimeStampMicroVector
    = new NullableTimeStampMicroVector("TimeStampValue", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), allocator)
  override val valueMutator: NullableTimeStampMicroVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
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
