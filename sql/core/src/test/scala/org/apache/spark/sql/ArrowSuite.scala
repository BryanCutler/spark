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

import java.io.File

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.file.json.JsonFileReader
import org.apache.arrow.vector.util.Validator
import org.apache.spark.sql.test.SharedSQLContext


private[sql] case class FloatData(a: Option[Float], b: Float)

class ArrowSuite extends SharedSQLContext {
  import testImplicits._

  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).getFile
  }

  test("collect to arrow record batch") {
    val data = Seq(1, 2, 3, 4, 5, 6).toDF()
    val arrowRecordBatch = data.collectAsArrow()
    assert(arrowRecordBatch.getLength > 0)
    assert(arrowRecordBatch.getNodes.size() > 0)
    arrowRecordBatch.close()
  }

  test("standard type conversion") {
    collectAndValidate(largeAndSmallInts, "test-data/arrow/largeAndSmall-ints.json")
    collectAndValidate(floatData, "test-data/arrow/floatData-single_precision.json")
    collectAndValidate(salary, "test-data/arrow/salary-doubles.json")
  }

  test("partitioned DataFrame") {
    collectAndValidate(testData2, "test-data/arrow/testData2-ints.json")
  }

  test("string type conversion") {
    collectAndValidate(upperCaseData, "test-data/arrow/uppercase-strings.json")
    collectAndValidate(lowerCaseData, "test-data/arrow/lowercase-strings.json")
  }

  test("time and date conversion") { }

  test("nested type conversion") { }

  test("array type conversion") {

  }

  test("mapped type conversion") { }

  test("other type conversion") {
    // byte type, or binary
    // allNulls

  }

  ignore("arbitrary precision floating point") {
    collectAndValidate(decimalData, "test-data/arrow/decimalData-BigDecimal.json")
  }

  test("other null conversion") { }

  test("convert int column with null to arrow") {
    collectAndValidate(nullInts, "test-data/arrow/null-ints.json")
    collectAndValidate(testData3, "test-data/arrow/null-ints-mixed.json")
  }

  test("convert string column with null to arrow") {
    val nullStringsColOnly = nullStrings.select(nullStrings.columns(1))
    collectAndValidate(nullStringsColOnly, "test-data/arrow/null-strings.json")
  }

  test("negative tests") { }

  /** Test that a converted DataFrame to Arrow record batch equals batch read from JSON file */
  private def collectAndValidate(df: DataFrame, arrowFile: String) {
    val jsonFilePath = testFile(arrowFile)

    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val jsonReader = new JsonFileReader(new File(jsonFilePath), allocator)

    val arrowSchema = Arrow.schemaToArrowSchema(df.schema)
    val jsonSchema = jsonReader.start()
    Validator.compareSchemas(arrowSchema, jsonSchema)

    val arrowRecordBatch = df.collectAsArrow(allocator)
    val arrowRoot = new VectorSchemaRoot(arrowSchema, allocator)
    val vectorLoader = new VectorLoader(arrowRoot)
    vectorLoader.load(arrowRecordBatch)
    val jsonRoot = jsonReader.read()

    Validator.compareVectorSchemaRoot(arrowRoot, jsonRoot)
  }

  protected lazy val floatData: DataFrame = {
    spark.sparkContext.parallelize(
      FloatData(Some(1), 1.0f) ::
      FloatData(None, 2.0f) ::
      FloatData(None, 0.01f) ::
      FloatData(Some(2), 200.0f) ::
      FloatData(None, 0.0001f) ::
      FloatData(Some(3), 20000.0f) :: Nil).toDF()
  }
}
