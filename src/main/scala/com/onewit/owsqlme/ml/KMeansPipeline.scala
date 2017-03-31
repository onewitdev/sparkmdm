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
package  com.onewit.owsqlme.ml

import com.onewit.owsqlme.config._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.spark.ml._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
//import org.apache.spark.ml.clustering.{KMeans,BisectingKMeans,GaussianMixture}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.NaiveBayes

import org.apache.spark.ml.{Pipeline, PipelineModel}

import com.rockymadden.stringmetric.similarity._
import com.rockymadden.stringmetric._

import scala.collection.mutable
import scala.language.reflectiveCalls

object KMeansPipeline {

  def range = 0 to 10
  def rnd = new scala.util.Random

  def rndString(inStr: String): String = {
  	var r = ""+inStr
  	val ri = rnd.nextInt(range length) 
  	if (ri<3 && r.length>0) r = r + r.last
  	r
  }
  
  def toUnicodeHex(inStr: String): Double = {
  	var h: Double = 0.0
  	inStr.getBytes() foreach (b => h = 0x100*h + b.toInt)
  	h
  }
      
/*
 * Main method provided here for debugging purposes, against local spark 
 */
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.master", "local") //local //spark://192.168.1.7:7077
      .config("spark.sql.warehouse.dir", "/tmp")
      .getOrCreate()
      
    args match {
      case Array(file, appId, useDCT, useBisecting,tokenLength,vecSize) =>
        run (spark,file,appId,useDCT=="true", useBisecting=="true", tokenLength.toInt, vecSize.toInt)      
      case Array(file, appId, useDCT, useBisecting,tokenLength) =>
        run (spark,file,appId,useDCT=="true", useBisecting=="true", tokenLength.toInt)      
      case Array(file, appId, useDCT, useBisecting) =>
        run (spark,file,appId,useDCT=="true", useBisecting=="true")
      case Array(file, appId, useDCT) =>
        run (spark,file,appId,useDCT=="true")
      case Array(file, appId) =>  
        run (spark,file,appId,false)
      case Array(file) =>
        val appId = ""+System.currentTimeMillis()
        run (spark,file,appId,false)
      case _ =>  
        run (spark,"file:///c:/tmp/npidata_20050523-20150412MAT1.dat", ""+System.currentTimeMillis(), true)
    }            
  }
   
  def run ( spark : SparkSession, 
          infile  : String, 
          appId   : String, 
          useDCT  : Boolean = true, 
          useBisecting : Boolean = false,
          tokenLength : Integer = 2,
          vecSize : Integer = 5
  ) : DataFrame = {
  
      val ngtnz = new NGramTokenizer(tokenLength)
      
      def getTokensAsStringArray( inStr : String) : Array[String] = {
        ngtnz.tokenize(inStr).getOrElse(Array(""))
      }
      
      def getTokensAsString( inStr : String) : String = {
        ngtnz.tokenize(inStr).map {as => as.mkString(" ")}.getOrElse("")
      }
  
      val npis = spark.sparkContext.textFile(infile)
      var arrnpis=  npis.map(_.split("\\|"))

      var toknsRows = arrnpis.zipWithIndex.map{ case (la, ln) => 
           val la0r = rndString(la(0))
           val la5r = rndString(la(5))
           val la6r = rndString(la(6))
           Row(
           ln.toInt,
           getTokensAsStringArray(la0r),
           getTokensAsStringArray(la5r),
           getTokensAsStringArray(la6r)
           //rndString(la(0)).toDouble,rndString(la(5)),rndString(la(6))
           )        
      }.cache()
      
      val k: Integer = toknsRows.count().toInt //toknsRows contains all the centreoids 
      
      //Taking each line from input, plus indexed rowid, 
      //we create 10 lines with some values randomly suffixed with last char
      var indexedInflatedtoknsRows = arrnpis.zipWithIndex.flatMap{case (la, ln) => 
         for(i<-0 to 9) yield {
           val la0r = rndString(la(0))
           val la5r = rndString(la(5))
           val la6r = rndString(la(6))           
           Row(ln.toInt*10 + i,la0r,la5r,la6r,
           getTokensAsStringArray(la0r),
           getTokensAsStringArray(la5r),
           getTokensAsStringArray(la6r)
           //rndString(la(0)).toDouble,rndString(la(5)),rndString(la(6))
           )
         }
      }.cache()
      
      val schema1 = StructType(
        Array(
          StructField("rowid",IntegerType,true), 
          StructField("npi_nga",ArrayType(StringType, true),true), 
          StructField("plname_nga",ArrayType(StringType, true),true), 
          StructField("pfname_nga",ArrayType(StringType, true),true)
        )
      )      

      //val Array(training, test) = df_indexedInflatedtoknsRows.randomSplit(Array(0.7, 0.3))
      val training = spark.createDataFrame(toknsRows, schema1)  
      
      
       val schema2 = StructType(
        Array(
          StructField("rowid",IntegerType,true), 
          StructField("npi",StringType, true), 
          StructField("plname",StringType, true), 
          StructField("pfname",StringType, true),
          StructField("npi_nga",ArrayType(StringType, true),true), 
          StructField("plname_nga",ArrayType(StringType, true),true), 
          StructField("pfname_nga",ArrayType(StringType, true),true)
        )
      )      
          
      val test = spark.createDataFrame(indexedInflatedtoknsRows, schema2)  

      val pipeline = createKMPipeline(
          k,
          useDCT, 
          useBisecting,
          tokenLength,
          vecSize) 

      val pipeModel = pipeline.fit(training)

      val predictionResult = pipeModel.transform(test)
      
      predictionResult.createOrReplaceTempView(s"tblClusters$appId")
     
      spark.sql(s"""drop table if exists tblClusters_pq$appId""")
      spark.sql(s"""create table tblClusters_pq$appId stored as parquet 
        as
        select 
          prediction eid,
          rowid mid,
          npi, 
          plname, 
          pfname 
        from tblClusters$appId""")
      
      modelEval (pipeModel , predictionResult)
          
      predictionResult.cache()
      //predictionResult.collect foreach (println)
  }

  def trainDf  ( spark : SparkSession, appId   : String, config: MDMConfig,
          df : DataFrame,
          useDCT  : Boolean = true, 
          useBisecting : Boolean = false,
          tokenLength : Integer = 2,
          vecSize : Integer = 5,
          sampleDivisor : Integer = 100
  )  = {
    
      //we are to sample df to produce train and test
      val dfTrain = df.where(s"rowid%${sampleDivisor}=0")
    
      val dfTest = df.where(s"rowid%${sampleDivisor}<10")
      
      val pipeline = createKMPipeline(
          dfTrain.count().toInt,
          useDCT, 
          useBisecting,
          tokenLength,
          vecSize) 

      val pipeModel = pipeline.fit(dfTrain)

      val predictionResult =  pipeModel.transform(dfTest)
      predictionResult.createOrReplaceTempView(s"tblClusters$appId")
     
      spark.sql(s"""drop table if exists tblClusters_pq$appId""")
      spark.sql(s"""create table tblClusters_pq$appId stored as parquet 
        as
        select 
          prediction eid,
          rowid mid,
          npi, 
          plname, 
          pfname 
        from tblClusters$appId""")
      
      modelEval (pipeModel , predictionResult)
          
      //predictionResult.cache()
      //predictionResult.collect foreach (println)       
  }
  
 
  def createKMPipeline(
          k: Integer, //# of centeroids
          useDCT  : Boolean = true, 
          useBisecting : Boolean = false,
          tokenLength : Integer = 2,
          vecSize : Integer = 5) 
      : Pipeline = {
         //increase vector size increases accuracy!!! minimum 3
      val word2Vec1 = new Word2Vec().setInputCol("npi_nga").setOutputCol("npiV").setVectorSize(vecSize).setMinCount(0)
      val word2Vec2 = new Word2Vec().setInputCol("plname_nga").setOutputCol("plnameV").setVectorSize(vecSize).setMinCount(0)
      val word2Vec3 = new Word2Vec().setInputCol("pfname_nga").setOutputCol("pfnameV").setVectorSize(vecSize).setMinCount(0)
      
      val ss1 = new StandardScaler().setInputCol("npiV").setOutputCol("npiVS")
      val ss2 = new StandardScaler().setInputCol("plnameV").setOutputCol("plnameVS")
      val ss3 = new StandardScaler().setInputCol("pfnameV").setOutputCol("pfnameVS")
      
      //val assembler = new VectorAssembler().setInputCols(Array("npi","plnameTF","pfnameTF")).setOutputCol("features0")
      //val vecassembler = new VectorAssembler().setInputCols(Array("npiV","plnameV","pfnameV"))
      val vecassembler = new VectorAssembler().setInputCols(Array("npiVS","plnameVS","pfnameVS"))
      .setOutputCol("features")
                   
      var kmeans : PipelineStage = new KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
      if (useBisecting) 
        kmeans = new BisectingKMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
      //Bisecting KMeans produce wrong clusters
      /* BisectingKMeans does not work at the moment:
      * 16/09/25 16:56:25 WARN scheduler.TaskSetManager: Lost task 1.0 in stage 60.0 (TID 116, 192.168.1.201): 
      * java.util.NoSuchElementException: key not found: 3     
      */
      //val gmm = new GaussianMixture().setK(k).setFeaturesCol("features").setPredictionCol("gmm_prediction")
      /*gmm cannot be piped at the moment  
        java.lang.IllegalArgumentException: requirement failed: Column features must be of type 
        	org.apache.spark.mllib.linalg.VectorUDT@f71b0bce 
        but was actually 
        	org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7.   
      */
      var stages = Array(
          word2Vec1,
          word2Vec2,
          word2Vec3,
          ss1,ss2,ss3,
          vecassembler,
          kmeans
          
      )
      useDCT match { 
        case true =>
          vecassembler.setOutputCol("features0")
          var dct = new DCT()
          .setInputCol("features0")
          .setOutputCol("features")
          .setInverse(false)
          stages = Array(
                word2Vec1,
                word2Vec2,
                word2Vec3,
                ss1,ss2,ss3,
                vecassembler,
                dct,
                kmeans               
            )      
        case _ => None
      }

      val pipeline = new Pipeline().setStages(stages)
       
      pipeline
  }
  
  def train  ( 
          df : DataFrame,
          p : Pipeline
  ) : PipelineModel = {
      p.fit(df)  
  }

  def predict  ( 
          df : DataFrame,
          pm : PipelineModel
  ) : DataFrame = {
      pm.transform(df)  
  }
  
  def modelEval (pipeModel :PipelineModel, predictionResult : DataFrame) {
         var WSSSE = 0.0
      var cluctrlen = 0      
      pipeModel.stages(pipeModel.stages.length - 1) match { //indexOf(kmeans)
        case km : KMeansModel => {
              WSSSE = km.computeCost(predictionResult)
              cluctrlen = km.clusterCenters.length
        }  
        case bkm : BisectingKMeansModel => {
              WSSSE = bkm.computeCost(predictionResult)
              cluctrlen = bkm.clusterCenters.length
        }
      }
  
      println(s"Within Set Sum of Squared Errors = $WSSSE")
      println(s"# of Cluster Centers: ${cluctrlen}") //foreach(println)
           
  }
 
}