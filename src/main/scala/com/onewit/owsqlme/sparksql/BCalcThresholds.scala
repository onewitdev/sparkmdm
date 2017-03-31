/*
Copyright 2016-2017, OneWit, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except 
in compliance with the License. 

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License 
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing permissions and limitations under the License.

Author:	roger.feng@onewit.com
*/
package com.onewit.owsqlme.sparksql

import com.onewit.owsqlme.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

import scala.collection.mutable._
import scala.language.reflectiveCalls

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel,BinaryLogisticRegressionSummary}
import org.apache.spark.sql.functions.max
import org.apache.spark.ml.feature.{StringIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.feature.VectorAssembler

//Use Logistic Regression to calculate threshold
object BCalcThresholds {
    
  case class Params(
    input: String = null,
    testInput: String = "",
    dataFormat: String = "libsvm",
    regParam: Double = 0.0,
    elasticNetParam: Double = 0.0,
    maxIter: Int = 100,
    fitIntercept: Boolean = true,
    tol: Double = 1E-6,
    fracTest: Double = 0.2)
      
  def thresholds (spark : SparkSession ,appId : String, config: MDMConfig) = {
  
    import spark.implicits._


    var tblsuffix = appId
    config.strategy match {
      case ResourceStrategy.Disk => {
        tblsuffix = "_pq"+appId
      }
      case _ => tblsuffix = appId
    }     
    
    // Load training data with labels. 
    // cl*.eid is the cluster id predicted by kmeans. if both items of a pair have the same cluster id, 
    // we label it a match
    var labelScoreSQL = s"""
      		select 
      		  case when cl1.eid=cl2.eid then 1 else 0 end 
      		  as label,
      		  sc_npi,sc_ln,sc_fn,
      		  score
      		from tblAllScores$tblsuffix sc
      		left outer join 
      		tblClusters_pq$appId cl1
      		on sc.mid1 = cl1.mid
      		left outer join 
      		tblClusters_pq$appId cl2
      		on sc.mid2 = cl2.mid
      		where sc.score>0 and cl1.mid is NOT null and cl2.mid is NOT NULL 

    """
    var training = spark.sql(labelScoreSQL)
    
    
    val stages = new ArrayBuffer[PipelineStage]()


    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
    stages += labelIndexer


    val vecassembler = new VectorAssembler().setInputCols(Array("sc_npi","sc_ln","sc_fn"))
      .setOutputCol("features")
  
    stages += vecassembler
      
    val params = Params()
    val lor = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setRegParam(params.regParam)
      .setElasticNetParam(params.elasticNetParam)
      .setMaxIter(params.maxIter)
      .setTol(params.tol)
      .setFitIntercept(params.fitIntercept)

    stages += lor
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipelineModel = pipeline.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

    val trainingSummary = lorModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = binarySummary.roc
    roc.show(50)
    println(binarySummary.areaUnderROC)

    // Set the model threshold to maximize F-Measure
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lorModel.setThreshold(bestThreshold)
    
    println(s"best threshold = ${bestThreshold}")

    
    var testSetSQL = s"""
      		select mid1,mid2,
      		  sc_npi,sc_ln,sc_fn,
      		  score
      		from tblAllScores$tblsuffix sc
      		where sc.score>0

    """
    var testSet = spark.sql(testSetSQL)
    val testSetlabeld = pipelineModel.transform(testSet) // lorModel.transform(training) -- no features column without pipeline
    testSetlabeld.createOrReplaceTempView(s"tblBinClassified$appId") 

    
    var tblsuffixO = appId
    config.strategy match {
      case ResourceStrategy.Disk => {
        tblsuffixO = "_pq"+appId
      }
      case _ => tblsuffixO = appId
    }    
    
    config.strategy match {
      case ResourceStrategy.None => None
      case ResourceStrategy.Cache => 
        testSetlabeld.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE tblBinClassified$appId")
      case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => 
        spark.sql(s"CACHE TABLE tblBinClassified$appId")
      case ResourceStrategy.Disk => {
        spark.sql(s"""drop table if exists tblBinClassified$tblsuffixO""") //$appId
        spark.sql(s"""create table tblBinClassified$tblsuffixO stored as parquet as select 
          mid1,mid2,
    		  sc_npi,sc_ln,sc_fn,
    		  score,prediction
          from tblBinClassified$appId""") //$appId
      } //end case
    } //end match          
 
  }
}