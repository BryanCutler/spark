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

package org.apache.spark.sql.test

import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, SQLImplicits}

private[sql] trait ArrowTestData {self =>
  protected def spark: SparkSession

  // Helper object to import SQL implicits without a concrete SQLContext
  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  import internalImplicits._
  import ArrowTestData._

  protected val arrowNullIntsFile = "test-data/arrowNullInts.json"
  protected val arrowNullStringsFile = "test-data/arrowNullStrings.json"

  protected lazy val arrowNullInts: DataFrame = spark.sparkContext.parallelize(
    NullInts(1) ::
        NullInts(2) ::
        NullInts(3) ::
        NullInts(null) :: Nil
  ).toDF()

  protected lazy val arrowNullStrings: DataFrame = spark.sparkContext.parallelize(
    NullStrings("a") ::
        NullStrings(null) ::
        NullStrings("b") ::
        NullStrings("ab") ::
        NullStrings("abc") ::
        NullStrings(null) :: Nil
  ).toDF()
}

/**
 * Case classes used in test data.
 */
private[sql] object ArrowTestData {
  case class NullInts(a: Integer)
  case class NullStrings(value: String)
}

