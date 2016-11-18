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

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowReader

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

case class ArrowIntTest(a: Int, b: Int)

class DatasetToArrowSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("Collect as arrow to python") {

    val ds = Seq(ArrowIntTest(1, 2), ArrowIntTest(2, 3), ArrowIntTest(3, 4)).toDS()

    val port = ds.collectAsArrowToPython()

    val receiver: RecordBatchReceiver = new RecordBatchReceiver
    val (buffer, numBytesRead) = receiver.connectAndRead(port)
    val channel = receiver.makeFile(buffer)
    val reader = new ArrowReader(channel, receiver.allocator)

    val footer = reader.readFooter()
    val schema = footer.getSchema
    assert(schema.getFields.size() === ds.schema.fields.length)
    assert(schema.getFields.get(0).getName === ds.schema.fields(0).name)
    assert(schema.getFields.get(0).isNullable === ds.schema.fields(0).nullable)
    assert(schema.getFields.get(1).getName === ds.schema.fields(1).name)
    assert(schema.getFields.get(1).isNullable === ds.schema.fields(1).nullable)

    val blockMetadata = footer.getRecordBatches
    assert(blockMetadata.size() === 1)

    val recordBatch = reader.readRecordBatch(blockMetadata.get(0))
    val nodes = recordBatch.getNodes
    assert(nodes.size() === 2)

    val firstNode = nodes.get(0)
    assert(firstNode.getLength === 3)
    assert(firstNode.getNullCount === 0)

    val buffers = recordBatch.getBuffers
    assert(buffers.size() === 4)

    val column1 = receiver.getIntArray(buffers.get(1))
    assert(column1=== Array(1, 2, 3))
    val column2 = receiver.getIntArray(buffers.get(3))
    assert(column2 === Array(2, 3, 4))
  }
}

class RecordBatchReceiver {

  val allocator = new RootAllocator(Long.MaxValue)

  def getIntArray(buf: ArrowBuf): Array[Int] = {
    val intBuf = ByteBuffer.wrap(array(buf)).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    val intArray = Array.ofDim[Int](intBuf.remaining())
    intBuf.get(intArray)
    intArray
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
