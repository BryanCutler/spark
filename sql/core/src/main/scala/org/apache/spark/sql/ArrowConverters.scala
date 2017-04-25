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

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}

import scala.collection.JavaConverters._

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BufferAllocator, BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector.BaseMutator
import org.apache.arrow.vector.file._
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.stream.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
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

/**
 * Store Arrow data in a form that can be serde by Spark
 */
private[sql] class ArrowPayload(val batchBytes: Array[Byte]) extends Serializable {

  def this(batch: ArrowRecordBatch, schema: StructType, allocator: BufferAllocator) = {
    this(ArrowConverters.batchToByteArray(batch, schema, allocator))
  }

  def loadBatch(allocator: BufferAllocator): ArrowRecordBatch = {
    ArrowConverters.byteArrayToBatch(batchBytes, allocator)
  }

  /*
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val rootAllocator = new RootAllocator(Long.MaxValue)
    _batch = MessageSerializer.deserializeMessageBatch(
      new ReadChannel(Channels.newChannel(in)), rootAllocator).asInstanceOf[ArrowRecordBatch]
    rootAllocator.close()
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch)
  }
  */
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
/*
private[sql] class ArrowConverters {
  private val _allocator = new RootAllocator(Long.MaxValue)

  private[sql] def allocator: RootAllocator = _allocator

  def interalRowIterToPayload(rowIter: Iterator[InternalRow], schema: StructType): ArrowPayload = {
    val batch = ArrowConverters.internalRowIterToArrowBatch(rowIter, schema, _allocator)
    new ArrowPayload(batch)
  }

  def close(): Unit = {
    _allocator.close()
  }
}
*/

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
      case DateType => new ArrowType.Date(DateUnit.DAY)
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
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

  /* REMOVE DOESN"T WORK
  private[sql] def writePayloads(payloadIter: Iterator[ArrowPayload], out: DataOutputStream): Unit = {
    payloadIter.foreach(payload => out.write(payload.batchBytes))
  }
  */

  private[sql] def writePayloads(
      payloadIter: Iterator[ArrowPayload],
      schema: StructType,
      out: DataOutputStream): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val loader = new VectorLoader(root)
    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))

    payloadIter.foreach { payload =>
      val batch = payload.loadBatch(allocator)
      loader.load(batch)
      writer.writeBatch()
      batch.close()
    }
    writer.end()
    root.close()
    allocator.close()
  }

  private[sql] def writePayloadsToFunc(
      func: ((Int, Array[ArrowPayload]) => Unit) => Unit,
      schema: StructType,
      out: DataOutputStream): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val loader = new VectorLoader(root)
    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))

    val payloadHandler = (partitionIndex: Int, payloads: Array[ArrowPayload]) => {
      payloads.foreach { payload =>
        val batch = payload.loadBatch(allocator)
        loader.load(batch)
        writer.writeBatch()
        batch.close()
      }
    }

    func(payloadHandler)

    writer.end()
    root.close()
    allocator.close()
  }

  private[sql] def writeRowsAsArrow(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      out: DataOutputStream): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val loader = new VectorLoader(root)
    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))

    val batch = internalRowIterToArrowBatch(rowIter, schema, allocator)

    // TODO: catch exceptions
    loader.load(batch)
    writer.writeBatch()
    writer.end()

    batch.close()
    root.close()
    allocator.close()
  }

  private[sql] def readArrowAsRows(in: DataInputStream): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      val _allocator = new RootAllocator(Long.MaxValue)
      private val _reader = new ArrowStreamReader(Channels.newChannel(in), _allocator)
      private val _root = _reader.getVectorSchemaRoot
      private var _index = 0
      val mutableRow = new GenericInternalRow(1)

      _reader.loadNextBatch()

      override def hasNext: Boolean = _index < _root.getRowCount

      override def next(): InternalRow = {
        val fields = _root.getFieldVectors.asScala

        /*
        val genericRowData = fields.map { field =>
          val obj: Any = field.getAccessor.getObject(_index)
          obj
        }.toArray*/

        _index += 1

        if (_index >= _root.getRowCount) {
          _index = 0
          _reader.loadNextBatch()
        }

        mutableRow(0) = fields.head.getAccessor.getObject(_index)
        mutableRow
        //new GenericInternalRow(genericRowData)
      }

      /*
      private def read(): ArrowRecordBatch = {
        _reader.loadNextBatch()
        if (_root.getRowCount > 0) {
          val unloader = new VectorUnloader(_root)
          unloader.getRecordBatch
        } else {
          null
        }
      }
      */
    }
  }


  /**
   * Maps Iterator from InternalRow to ArrowPayload
   */
  private[sql] def toPayloadIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType): Iterator[ArrowPayload] = {
    new Iterator[ArrowPayload] {
      private val _allocator = new RootAllocator(Long.MaxValue)
      private var _nextPayload = if (rowIter.nonEmpty) convert() else null

      override def hasNext: Boolean = _nextPayload != null

      override def next(): ArrowPayload = {
        val obj = _nextPayload
        if (hasNext) {
          if (rowIter.hasNext) {
            _nextPayload = convert()
          } else {
            _allocator.close()
            _nextPayload = null
          }
        }
        obj
      }

      private def convert(): ArrowPayload = {
        val batch = internalRowIterToArrowBatch(rowIter, schema, _allocator)
        new ArrowPayload(batch, schema, _allocator)
      }
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
      ColumnWriter(ordinal, allocator, field.dataType).init()
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
   * Convert an ArrowRecordBatch to a byte array and close batch
   */
  private[sql] def batchToByteArray(
      batch: ArrowRecordBatch,
      schema: StructType,
      allocator: BufferAllocator): Array[Byte] = {
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))

    // Iterate over payload batches to write each one, ensure all batches get closed
    Utils.tryWithSafeFinally {
      val loader = new VectorLoader(root)
      loader.load(batch)
      writer.writeBatch()
    } {
      batch.close()
      root.close()
      writer.close()
    }
    out.toByteArray
  }

  /**
   * Convert a byte array to an ArrowRecordBatch
   */
  private[sql] def byteArrayToBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayReadableSeekableByteChannel(batchBytes)
    val reader = new ArrowFileReader(in, allocator)
    val root = reader.getVectorSchemaRoot
    val unloader = new VectorUnloader(root)
    reader.loadNextBatch()
    val batch = unloader.getRecordBatch
    reader.close()
    batch
  }

  /*
  def readPayloadByteArrays(payloadByteArrays: Array[Array[Byte]], allocator: BufferAllocator): Iterator[ArrowPayload] = {
    // TODO: change to real iterator, to get one payload at a time
    val batches = scala.collection.mutable.ArrayBuffer.empty[ArrowPayload]
    var i = 0
    while (i < payloadByteArrays.length) {
      val payloadBytes = payloadByteArrays(i)
      val in = new ByteArrayReadableSeekableByteChannel(payloadBytes)
      val reader = new ArrowFileReader(in, allocator)
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
                    out: OutputStream,
                    allocator: BufferAllocator): Unit = {

    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
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

  def readPayloads(in: InputStream, allocator: BufferAllocator): Iterator[ArrowPayload] = {
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

  def payloadToInternalRowIter(payload: ArrowPayload, schema: StructType, allocator: BufferAllocator): Iterator[InternalRow] = {
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
  }*/
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
private[sql] abstract class PrimitiveColumnWriter(val ordinal: Int, val allocator: BaseAllocator)
  extends ColumnWriter {

  def getFieldType(arrowType: ArrowType): FieldType = FieldType.nullable(arrowType)

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

private[sql] class BooleanColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableBitVector
    = new NullableBitVector("BooleanValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableBitVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, if (row.getBoolean(ordinal)) 1 else 0 )
}

private[sql] class ShortColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableSmallIntVector
    = new NullableSmallIntVector("ShortValue", getFieldType(arrowType: ArrowType), allocator)
  override val valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getShort(ordinal))
}

private[sql] class IntegerColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableIntVector
    = new NullableIntVector("IntValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getInt(ordinal))
}

private[sql] class LongColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableBigIntVector
    = new NullableBigIntVector("LongValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getLong(ordinal))
}

private[sql] class FloatColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableFloat4Vector
    = new NullableFloat4Vector("FloatValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableFloat4Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getFloat(ordinal))
}

private[sql] class DoubleColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableFloat8Vector
    = new NullableFloat8Vector("DoubleValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableFloat8Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getDouble(ordinal))
}

private[sql] class ByteColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableUInt1Vector
    = new NullableUInt1Vector("ByteValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableUInt1Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getByte(ordinal))
}

private[sql] class UTF8StringColumnWriter(
    arrowType: ArrowType,
    ordinal: Int,
    allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("UTF8StringValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    val bytes = row.getUTF8String(ordinal).getBytes
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class BinaryColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("BinaryValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    val bytes = row.getBinary(ordinal)
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class DateColumnWriter(arrowType: ArrowType, ordinal: Int, allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableDateDayVector
    = new NullableDateDayVector("DateValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableDateDayVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    valueMutator.setSafe(count, row.getInt(ordinal))
  }
}

private[sql] class TimeStampColumnWriter(
    arrowType: ArrowType,
    ordinal: Int,
    allocator: BaseAllocator)
  extends PrimitiveColumnWriter(ordinal, allocator) {
  override val valueVector: NullableTimeStampMicroVector
    = new NullableTimeStampMicroVector("TimeStampValue", getFieldType(arrowType), allocator)
  override val valueMutator: NullableTimeStampMicroVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    valueMutator.setSafe(count, row.getLong(ordinal))
  }
}

private[sql] object ColumnWriter {
  def apply(ordinal: Int, allocator: BaseAllocator, dataType: DataType): ColumnWriter = {
    val arrowType = ArrowConverters.sparkTypeToArrowType(dataType)
    dataType match {
      case BooleanType => new BooleanColumnWriter(arrowType, ordinal, allocator)
      case ShortType => new ShortColumnWriter(arrowType, ordinal, allocator)
      case IntegerType => new IntegerColumnWriter(arrowType, ordinal, allocator)
      case LongType => new LongColumnWriter(arrowType, ordinal, allocator)
      case FloatType => new FloatColumnWriter(arrowType, ordinal, allocator)
      case DoubleType => new DoubleColumnWriter(arrowType, ordinal, allocator)
      case ByteType => new ByteColumnWriter(arrowType, ordinal, allocator)
      case StringType => new UTF8StringColumnWriter(arrowType, ordinal, allocator)
      case BinaryType => new BinaryColumnWriter(arrowType, ordinal, allocator)
      case DateType => new DateColumnWriter(arrowType, ordinal, allocator)
      case TimestampType => new TimeStampColumnWriter(arrowType, ordinal, allocator)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}
