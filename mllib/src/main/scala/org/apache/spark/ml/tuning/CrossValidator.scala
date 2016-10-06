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

package org.apache.spark.ml.tuning

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.github.fommil.netlib.F2jBLAS
import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

/**
 * Params for [[CrossValidator]] and [[CrossValidatorModel]].
 */
private[ml] trait CrossValidatorParams extends ValidatorParams {
  /**
   * Param for number of folds for cross validation.  Must be &gt;= 2.
   * Default: 3
   *
   * @group param
   */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)

  val numParallelEval: IntParam = new IntParam(this, "numParallelEval",
    "max number of models to evaluate in parallel, 1 for serial evaluation")

  val optimizePipeline: BooleanParam = new BooleanParam(this, "optimizePipeline",
    "optimize evaluation of a pipeline with Param grid")

  setDefault(numParallelEval -> Int.MaxValue, optimizePipeline -> true)
}

/**
 * K-fold cross validation performs model selection by splitting the dataset into a set of
 * non-overlapping randomly partitioned folds which are used as separate training and test datasets
 * e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
 * each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
 * test set exactly once.
 */
@Since("1.2.0")
class CrossValidator @Since("1.2.0") (@Since("1.4.0") override val uid: String)
  extends Estimator[CrossValidatorModel]
  with CrossValidatorParams with MLWritable with Logging {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("cv"))

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  @Since("1.2.0")
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.1.0")
  def setNumParallelEval(value: Int): this.type = set(numParallelEval, value)

  /** @group setParam */
  @Since("2.1.0")
  def setOptimizePipeline(value: Boolean): this.type = set(optimizePipeline, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val isPipelineAndOptimize = est.isInstanceOf[Pipeline] && $(optimizePipeline)
    
    val instr = Instrumentation.create(this, dataset)
    instr.logParams(numFolds, seed)
    logTuningParams(instr)

    val metrics = ($(numParallelEval), isPipelineAndOptimize) match {
      case (n, _) if n <= 1 =>
        fitSerial(dataset)
      case (_, false) =>
        fitParallel(dataset)
      case (_, true) =>
        fitPipelineOptimized(dataset, est.asInstanceOf[Pipeline])
    }

    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    logError(s"*** metric: ${metrics.mkString(",")}")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    instr.logSuccess(bestModel)
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }

  private def fitSerial(dataset: Dataset[_]): Array[Double] = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)

    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    logDebug("Running serial cross-validation.")
    splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      trainingDataset.unpersist()
      var i = 0
      while (i < numModels) {
        // TODO: duplicate evaluator to take extra params from input
        val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
        logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
      }

      validationDataset.unpersist()
    }

    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    metrics
  }

  private def fitParallel(dataset: Dataset[_]): Array[Double] = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val numPar = $(numParallelEval)
    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    logDebug("Running parallelized cross-validation.")

    // Compute metrics for each model over each fold
    val metrics = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")

      // Fit models concurrently, limited by using a sliding window over models
      val models = epm.grouped(numPar).map { win =>
        win.par.map(est.fit(trainingDataset, _))
      }.toList.flatten.asInstanceOf[Seq[Model[_]]]
      trainingDataset.unpersist()

      // Evaluated models concurrently, limited by using a sliding window over models
      val foldMetrics = models.zip(epm).grouped(numPar).map { win =>
        win.par.map { m =>
          // TODO: duplicate evaluator to take extra params from input
          val metric = eval.evaluate(m._1.transform(validationDataset, m._2))
          logDebug(s"Got metric $metric for model trained with ${m._2}.")
          metric
        }
      }.toList.flatten

      validationDataset.unpersist()
      foldMetrics
    }.reduce((mA, mB) => mA.zip(mB).map(m => m._1 + m._2)).toArray

    // Calculate average metric for all folds
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    metrics
  }

  private def fitPipelineOptimized(dataset: Dataset[_], est: Pipeline): Array[Double] = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val numPar = $(numParallelEval)
    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    logDebug("Running optimized cross-validation for a Pipeline model.")

    // Implements transformation to go from one Node to another
    class Edge(stage: PipelineStage, params: ParamMap, parent: Node) {
      private var transformer: Transformer = null
      def fit(): Unit = {
        if (transformer == null) {
          transformer = stage match {
            case e: Estimator[_] =>
              e.fit(parent.fit().transform(), params)
            case t: Transformer =>
              parent.fit()
              t
          }
        }
      }
      def transform(): DataFrame = {
        transformer.transform(parent.transform(), params)
      }
      def clear(clearOnlyData: Boolean): Unit = {
        if (!clearOnlyData) transformer = null
        if (parent != null) parent.clear(clearOnlyData)
      }
    }

    // Stores a dataset that can be reused by child Nodes
    class Node(edge: Edge, val params: ParamMap) {
      private var dataset: DataFrame = null
      def setDataset(ds: DataFrame): Unit = {
        dataset = ds
      }
      def fit(): Node = {
        if (edge != null) edge.fit()
        this
      }
      def transform(): DataFrame = {
        if (dataset == null) {
          dataset = edge.transform()
        }
        dataset
      }
      def clear(clearOnlyData: Boolean = false): Unit = {
        dataset = null
        if (edge != null) edge.clear(clearOnlyData)  // TODO - may not need to clear parent data
      }
    }

    val theStages = est.getStages

    // Separate model ParamMaps to corresponding stages
    val stageParams = theStages.map { stage =>
      val spm = epm.map { param =>
        param.filter(stage).toList
      }.distinct.map { paramList =>
        new ParamMap().put(paramList)
      }
      spm
    }

    // Build tree - Node is a dataset, Edge is a transformer with applied params,
    // the path from root to a leaf is a PiplineModel
    val root = new Node(null, ParamMap.empty)
    var leaves = Array[Node](root)

    theStages.zip(stageParams).foreach { case (stage, spm) =>
      val tempNodes = new ListBuffer[Node]()
      leaves.foreach { parent =>
        spm.foreach { params =>
          val parentEdge = new Edge(stage, params, parent)
          tempNodes += new Node(parentEdge, params ++ parent.params)
        }
      }
      leaves = tempNodes.toArray
    }

    // Compute metrics for each fold
    val metrics = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()

      // will train multiple pipeline models
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")

      // set tree root to the training dataset
      root.setDataset(trainingDataset)

      // fit the PipelineModel
      leaves.foreach(_.fit())

      // change tree dataset to now use validation dataset
      leaves.foreach(_.clear(clearOnlyData = true))
      trainingDataset.unpersist()
      root.setDataset(validationDataset)

      // evaluate each model
      val foldMetrics = leaves.map { leaf =>
        eval.evaluate(leaf.transform())
      }

      // clear fit and data in tree to process next fold
      leaves.foreach(_.clear())
      validationDataset.unpersist()

      foldMetrics
    }.reduce((mA, mB) => mA.zip(mB).map(m => m._1 + m._2))

    // Calculate average metric for all folds
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)

    // leaves don't match up with model index, so need to put back in original order
    // first sort by ParamMap string (which is ordered by name) then zip with metrics
    // TODO - can probably find a cleaner way to do this
    val sortedMetrics = leaves.map(node => node.params.toString).zip(metrics).sortBy(_._1).map(_._2)
    val orderedMetrics = epm.map(params => params.toString).zipWithIndex.sortBy(_._1).map(_._2)
        .zip(sortedMetrics).sortBy(_._1).map(_._2)

    orderedMetrics
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[CrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
  // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
  // However, this case should be unusual.
  @Since("1.6.0")
  override def write: MLWriter = new CrossValidator.CrossValidatorWriter(this)
}

@Since("1.6.0")
object CrossValidator extends MLReadable[CrossValidator] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidator] = new CrossValidatorReader

  @Since("1.6.0")
  override def load(path: String): CrossValidator = super.load(path)

  private[CrossValidator] class CrossValidatorWriter(instance: CrossValidator) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class CrossValidatorReader extends MLReader[CrossValidator] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidator].getName

    override def load(path: String): CrossValidator = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      new CrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
        .setNumFolds(numFolds)
        .setSeed(seed)
    }
  }
}

/**
 * CrossValidatorModel contains the model with the highest average cross-validation
 * metric across folds and uses this model to transform input data. CrossValidatorModel
 * also tracks the metrics for each param map evaluated.
 *
 * @param bestModel The best model selected from k-fold cross validation.
 * @param avgMetrics Average cross-validation metrics for each paramMap in
 *                   `CrossValidator.estimatorParamMaps`, in the corresponding order.
 */
@Since("1.2.0")
class CrossValidatorModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("1.2.0") val bestModel: Model[_],
    @Since("1.5.0") val avgMetrics: Array[Double])
  extends Model[CrossValidatorModel] with CrossValidatorParams with MLWritable {

  /** A Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) = {
    this(uid, bestModel, avgMetrics.asScala.toArray)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidatorModel = {
    val copied = new CrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new CrossValidatorModel.CrossValidatorModelWriter(this)
}

@Since("1.6.0")
object CrossValidatorModel extends MLReadable[CrossValidatorModel] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidatorModel] = new CrossValidatorModelReader

  @Since("1.6.0")
  override def load(path: String): CrossValidatorModel = super.load(path)

  private[CrossValidatorModel]
  class CrossValidatorModelWriter(instance: CrossValidatorModel) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "avgMetrics" -> instance.avgMetrics.toSeq
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class CrossValidatorModelReader extends MLReader[CrossValidatorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidatorModel].getName

    override def load(path: String): CrossValidatorModel = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
      val model = new CrossValidatorModel(metadata.uid, bestModel, avgMetrics)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
        .set(model.numFolds, numFolds)
        .set(model.seed, seed)
    }
  }
}
