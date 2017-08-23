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

package org.apache.spark.sql.execution.arrow

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.file._
import org.apache.arrow.vector.schema.ArrowRecordBatch
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ArrowColumnVector, ColumnarBatch, ReadOnlyColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * Store Arrow data in a form that can be serialized by Spark and served to a Python process.
 */
private[sql] class ArrowPayload private[sql] (payload: Array[Byte]) extends Serializable {

  /**
   * Convert the ArrowPayload to an ArrowRecordBatch.
   */
  def loadBatch(allocator: BufferAllocator): ArrowRecordBatch = {
    ArrowConverters.byteArrayToBatch(payload, allocator)
  }

  /**
   * Get the ArrowPayload as a type that can be served to Python.
   */
  def asPythonSerializable: Array[Byte] = payload
}

private[sql] object ArrowConverters {

  /**
   * Maps Iterator from InternalRow to ArrowPayload. Limit ArrowRecordBatch size in ArrowPayload
   * by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toPayloadIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int,
      context: TaskContext): Iterator[ArrowPayload] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toPayloadIterator", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)

    var closed = false

    context.addTaskCompletionListener { _ =>
      if (!closed) {
        root.close()
        allocator.close()
      }
    }

    new Iterator[ArrowPayload] {

      override def hasNext: Boolean = rowIter.hasNext || {
        root.close()
        allocator.close()
        closed = true
        false
      }

      override def next(): ArrowPayload = {
        val out = new ByteArrayOutputStream()
        val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))

        Utils.tryWithSafeFinally {
          var rowCount = 0
          while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            val row = rowIter.next()
            arrowWriter.write(row)
            rowCount += 1
          }
          arrowWriter.finish()
          writer.writeBatch()
        } {
          arrowWriter.reset()
          writer.close()
        }

        new ArrowPayload(out.toByteArray)
      }
    }
  }

  private[sql] def fromPayloadIterator(
      payloadIter: Iterator[ArrowPayload],
      context: TaskContext): (Iterator[InternalRow], StructType) = {

    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromPayloadIterator", 0, Long.MaxValue)
    var reader: ArrowFileReader = null

    def nextBatch(): (Iterator[InternalRow], StructType) = {
      val in = new ByteArrayReadableSeekableByteChannel(payloadIter.next().asPythonSerializable)
      reader = new ArrowFileReader(in, allocator)
      reader.loadNextBatch()  // throws IOException
      val root = reader.getVectorSchemaRoot  // throws IOException
      val schemaRead = ArrowUtils.fromArrowSchema(root.getSchema)

      val columns = root.getFieldVectors.asScala.map { vector =>
        new ArrowColumnVector(vector).asInstanceOf[ReadOnlyColumnVector]
      }.toArray

      (ColumnarBatch.createReadOnly(schemaRead, columns, root.getRowCount).rowIterator().asScala,
        schemaRead)
    }

    var (rowIter, schemaRead) = if (payloadIter.hasNext) {
      nextBatch()
    } else {
      (Iterator.empty, StructType(Seq.empty))
    }

    val outputIterator = new Iterator[InternalRow] {

      context.addTaskCompletionListener { _ =>
        closeReader()
        allocator.close()
      }

      override def hasNext: Boolean = rowIter.hasNext || {
        closeReader()
        if (payloadIter.hasNext) {
          rowIter = nextBatch()._1
          true
        } else {
          allocator.close()
          false
        }
      }

      override def next(): InternalRow = rowIter.next()

      private def closeReader(): Unit = {
        if (reader != null) {
          reader.close()
          reader = null
        }
      }
    }

    (outputIterator, schemaRead)
  }

  /**
   * Convert a byte array to an ArrowRecordBatch.
   */
  private[arrow] def byteArrayToBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayReadableSeekableByteChannel(batchBytes)
    val reader = new ArrowFileReader(in, allocator)

    // Read a batch from a byte stream, ensure the reader is closed
    Utils.tryWithSafeFinally {
      val root = reader.getVectorSchemaRoot  // throws IOException
      val unloader = new VectorUnloader(root)
      reader.loadNextBatch()  // throws IOException
      unloader.getRecordBatch
    } {
      reader.close()
    }
  }

  def toDataFrame(arrowRDD: JavaRDD[Array[Byte]], sqlContext: SQLContext): DataFrame = {
    val rdd = arrowRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromPayloadIterator(iter.map(new ArrowPayload(_)), context)._1
    }

    val schemaList = arrowRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      if (iter.hasNext) {
        val first = iter.next()
        Iterator(ArrowConverters.fromPayloadIterator(Iterator(new ArrowPayload(first)), context)._2)
      } else {
        Iterator.empty
      }
    }.collect()

    val schema = if (schemaList.nonEmpty) schemaList(0) else StructType(Seq.empty)

    sqlContext.internalCreateDataFrame(rdd, schema)
  }
}
