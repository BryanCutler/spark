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
import org.apache.arrow.vector.VarBinaryVector

import org.apache.spark.unsafe.types.UTF8String

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
