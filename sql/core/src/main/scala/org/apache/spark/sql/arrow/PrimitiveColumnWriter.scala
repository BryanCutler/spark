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

package org.apache.spark.sql.arrow

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.BaseAllocator
import org.apache.arrow.vector.{BaseDataValueVector, BitVector}
import org.apache.arrow.vector.BaseValueVector.BaseMutator
import org.apache.arrow.vector.schema.ArrowFieldNode

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
