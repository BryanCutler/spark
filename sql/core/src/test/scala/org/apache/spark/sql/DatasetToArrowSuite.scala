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

import java.io._
import java.net.{InetAddress, Socket}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel

import scala.util.Random

import io.netty.buffer.ArrowBuf
import org.apache.arrow.flatbuf.Precision
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowReader
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


case class ArrowTestClass(a: Int, b: Double, c: String)

class DatasetToArrowSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  final val numElements = 4
  @transient var dataset: Dataset[_] = _
  @transient var column1: Seq[Int] = _
  @transient var column2: Seq[Double] = _
  @transient var column3: Seq[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    column1 = Seq.fill(numElements)(Random.nextInt)
    column2 = Seq.fill(numElements)(Random.nextDouble)
    column3 = Seq.fill(numElements)(Random.nextString(Random.nextInt(100)))
    dataset = column1.zip(column2).zip(column3)
      .map{ case ((c1, c2), c3) => ArrowTestClass(c1, c2, c3) }.toDS()
  }

  test("Collect as arrow to python") {

    val port = dataset.collectAsArrowToPython()

    val receiver: RecordBatchReceiver = new RecordBatchReceiver
    val (buffer, numBytesRead) = receiver.connectAndRead(port)
    val channel = receiver.makeFile(buffer)
    val reader = new ArrowReader(channel, receiver.allocator)

    val footer = reader.readFooter()
    val schema = footer.getSchema
    val numCols = schema.getFields.size()
    assert(numCols === dataset.schema.fields.length)
    for (i <- 0 to schema.getFields.size()) {
      val arrowField = schema.getFields.get(i)
      val sparkField = dataset.schema.fields(i)
      assert(arrowField.getName === sparkField.name)
      assert(arrowField.isNullable === sparkField.nullable)
      assert(DatasetToArrowSuite.compareSchemaTypes(arrowField.getType, sparkField.dataType))
    }

    val blockMetadata = footer.getRecordBatches
    assert(blockMetadata.size() === 1)

    val recordBatch = reader.readRecordBatch(blockMetadata.get(0))
    val nodes = recordBatch.getNodes
    assert(nodes.size() === numCols)

    val firstNode = nodes.get(0)
    assert(firstNode.getLength === numElements)
    assert(firstNode.getNullCount === 0)

    val buffers = recordBatch.getBuffers
    assert(buffers.size() === numCols * 2)

    val column1Read = receiver.getIntArray(buffers.get(1))
    assert(column1Read === column1)
    val column2Read = receiver.getDoubleArray(buffers.get(3))
    assert(column2Read === column2)
    // TODO: Check column 3 is right
  }
}

object DatasetToArrowSuite {
  def compareSchemaTypes(at: ArrowType, dt: DataType): Boolean = {
    (at, dt) match {
      case (_: ArrowType.Int, _: IntegerType) => true
      case (_: ArrowType.FloatingPoint, _: DoubleType) =>
        at.asInstanceOf[ArrowType.FloatingPoint].getPrecision == Precision.DOUBLE
      case (_: ArrowType.FloatingPoint, _: FloatType) =>
        at.asInstanceOf[ArrowType.FloatingPoint].getPrecision == Precision.SINGLE
      case (_: ArrowType.Utf8, _: StringType) => true
      case (_: ArrowType.Bool, _: BooleanType) => true
      case _ => false
    }
  }
}

class RecordBatchReceiver {

  val allocator = new RootAllocator(Long.MaxValue)

  def getIntArray(buf: ArrowBuf): Array[Int] = {
    val buffer = ByteBuffer.wrap(array(buf)).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    val resultArray = Array.ofDim[Int](buffer.remaining())
    buffer.get(resultArray)
    resultArray
  }

  def getDoubleArray(buf: ArrowBuf): Array[Double] = {
    val buffer = ByteBuffer.wrap(array(buf)).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer()
    val resultArray = Array.ofDim[Double](buffer.remaining())
    buffer.get(resultArray)
    resultArray
  }

  def getStringArray(buf: ArrowBuf): Array[String] = {
    val buffer = ByteBuffer.wrap(array(buf)).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer()
    val resultArray = Array.ofDim[String](buffer.remaining())
    // TODO: Get String Array back
    resultArray
  }

  private def array(buf: ArrowBuf): Array[Byte] = {
    val bytes = Array.ofDim[Byte](buf.readableBytes())
    buf.readBytes(bytes)
    bytes
  }

  def connectAndRead(port: Int): (Array[Byte], Int) = {
    val clientSocket = new Socket(InetAddress.getByName("localhost"), port)
    val clientDataIns = new DataInputStream(clientSocket.getInputStream)

    val messageLength = clientDataIns.readInt()

    val buffer = Array.ofDim[Byte](messageLength)
    val bytesRead = clientDataIns.read(buffer)
    if (bytesRead != messageLength) {
      throw new EOFException("Wrong EOF to read Arrow Bytes")
    }
    (buffer, messageLength)
  }

  def makeFile(buffer: Array[Byte]): FileChannel = {
    val tempDir = Utils.createTempDir(namePrefix = this.getClass.getName).getPath
    val arrowFile = new File(tempDir, "arrow-bytes")
    val arrowOus = new FileOutputStream(arrowFile.getPath)
    arrowOus.write(buffer)
    arrowOus.close()

    val arrowIns = new FileInputStream(arrowFile.getPath)
    arrowIns.getChannel
  }
}
