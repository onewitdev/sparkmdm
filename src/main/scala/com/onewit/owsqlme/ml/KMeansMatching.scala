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


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

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

import org.apache.spark.ml.{Pipeline, PipelineModel}

import com.rockymadden.stringmetric.similarity._
import com.rockymadden.stringmetric._


object KMeansMatching {

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
      

  def matching (spark : SparkSession, model: KMeansModel, infile: String , appId: String) : DataFrame = {

      val npis = spark.sparkContext.textFile(infile)
      var arrnpis=  npis.map(_.split("\\|"))
      
      //Taking each line from input, plus indexed rowid, 
      //we create 10 lines with some values randomly suffixed with last char
      var rndnpis = arrnpis.zipWithIndex.flatMap{case (la, ln) => 
         for(i<-0 to 9) yield { 
           Row(ln.toInt*10 + i,
           rndString(la(0)),rndString(la(5)),rndString(la(6))
           )
         }
      }
      var svmRow = rndnpis.map(la => 
      ""+la(0)+" 1:"+la(1)+" 2:"+ toUnicodeHex(""+la(2))+" 3:"+ toUnicodeHex(""+la(3)))
         
      val outfile = infile+"_X10_2svm_"+appId
      svmRow.coalesce(1,true).saveAsTextFile(outfile) //"file:///c:/tmp/libsvm");

      // $example on$
      // Can we create a dataset here directly???
        
      val dataset = spark.read.format("libsvm").load(outfile)
      dataset.cache()
  
      //How do we use the already trained model(containing the centeroids) for clustering matching rows togather???
      val ret = model.transform(dataset)
      ret
      //spark.stop()
  }
  
  
  def pipeline0 (spark : SparkSession, infile: String , appId: String) : DataFrame = {

  
      val npis = spark.sparkContext.textFile(infile)
      var arrnpis=  npis.map(_.split("\\|"))

      var hexedRows = arrnpis.zipWithIndex.map{ case (la, ln) => 
           Row(
           ln.toInt,
           rndString(la(0)).toDouble,
           toUnicodeHex(
               rndString(la(5))
           )
           ,
           toUnicodeHex(
               rndString(la(6))
           )
           //    rndString(la(0)).toDouble,rndString(la(5)),rndString(la(6))
           )        
      }
      val k: Integer = hexedRows.count().toInt //hexedRows contains all the centreoids 
      
      //Taking each line from input, plus indexed rowid, 
      //we create 10 lines with some values randomly suffixed with last char
      var indexedInflatedHexedRows = arrnpis.zipWithIndex.flatMap{case (la, ln) => 
         for(i<-0 to 9) yield { 
           Row(ln.toInt*10 + i,
           rndString(la(0)).toDouble, //toUnicodeHex
           toUnicodeHex(
               rndString(la(5))
           )
           ,
           toUnicodeHex(
               rndString(la(6))
           )
           //    rndString(la(0)).toDouble,rndString(la(5)),rndString(la(6))
           )
         }
      }
      val schema = StructType(
        Array(
          StructField("rowid",IntegerType,true), 
          StructField("npi",DoubleType,true), 
          StructField("plname",DoubleType,true), 
          StructField("pfname",DoubleType,true)
        )
      )      

      //val Array(training, test) = df_indexedInflatedHexedRows.randomSplit(Array(0.7, 0.3))
      val training = spark.createDataFrame(hexedRows, schema)  
      val test = spark.createDataFrame(indexedInflatedHexedRows, schema)  

      //val assembler = new VectorAssembler().setInputCols(Array("npi","plnameTF","pfnameTF")).setOutputCol("features0")
      val vecassembler = new VectorAssembler().setInputCols(Array("npi","plname","pfname")).setOutputCol("features0")
               
      
      val dct = new DCT()
        .setInputCol("features0")
        .setOutputCol("features")
        .setInverse(false)

      val kmeans = new  KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")

      val gmm = new GaussianMixture().setK(k).setFeaturesCol("features").setPredictionCol("gmm_prediction")
      
      val pipeline = new Pipeline().setStages(
          Array(
            vecassembler, 
            dct,
            kmeans
          ))
       
      val pipeModel = pipeline.fit(training)

      
      val predictionResult = pipeModel.transform(test)
      predictionResult.collect foreach (println)
      
      val kmModel : KMeansModel = pipeModel.stages(2).asInstanceOf[KMeansModel]
    
      val WSSSE = kmModel.computeCost(predictionResult)
      println(s"Within Set Sum of Squared Errors = $WSSSE")
      println("Cluster Centers: ")
      kmModel.clusterCenters.foreach(println)
      predictionResult
  }


  def tokenLength : Integer = 3
  def ngtnz = new NGramTokenizer(tokenLength)
  
  def getTokensAsStringArray( inStr : String) : Array[String] = {
    ngtnz.tokenize(inStr).getOrElse(Array(""))
  }
  
  def getTokensAsString( inStr : String) : String = {
    ngtnz.tokenize(inStr).map {as => as.mkString(" ")}.getOrElse("")
  }
  
  def pipeline (spark : SparkSession, infile: String , appId: String, useDCT: Boolean = false) : DataFrame = {
 
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
          StructField("npi",ArrayType(StringType, true),true), 
          StructField("plname",ArrayType(StringType, true),true), 
          StructField("pfname",ArrayType(StringType, true),true)
        )
      )      

      //val Array(training, test) = df_indexedInflatedtoknsRows.randomSplit(Array(0.7, 0.3))
      val training = spark.createDataFrame(toknsRows, schema1)  
      
      
       val schema2 = StructType(
        Array(
          StructField("rowid",IntegerType,true), 
          StructField("snpi",StringType, true), 
          StructField("splname",StringType, true), 
          StructField("spfname",StringType, true),
          StructField("npi",ArrayType(StringType, true),true), 
          StructField("plname",ArrayType(StringType, true),true), 
          StructField("pfname",ArrayType(StringType, true),true)
        )
      )      
          
      val test = spark.createDataFrame(indexedInflatedtoknsRows, schema2)  

      //increase vector size increases accuracy!!! minimum 3
      val vecSize : Integer = 3
      val word2Vec1 = new Word2Vec().setInputCol("npi").setOutputCol("npiV").setVectorSize(vecSize).setMinCount(0)
      val word2Vec2 = new Word2Vec().setInputCol("plname").setOutputCol("plnameV").setVectorSize(vecSize).setMinCount(0)
      val word2Vec3 = new Word2Vec().setInputCol("pfname").setOutputCol("pfnameV").setVectorSize(vecSize).setMinCount(0)
      
      //val assembler = new VectorAssembler().setInputCols(Array("npi","plnameTF","pfnameTF")).setOutputCol("features0")
      val vecassembler = new VectorAssembler().setInputCols(Array("npiV","plnameV","pfnameV"))
      .setOutputCol("features")
                     


      val kmeans = new KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")

      var stages = Array(
          word2Vec1,
          word2Vec2,
          word2Vec3,
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
                vecassembler,
                dct,
                kmeans               
            )      
        case _ => None
      }

      val pipeline = new Pipeline().setStages(stages)
       
      val pipeModel = pipeline.fit(training)

      val predictionResult = pipeModel.transform(test)
      predictionResult.createOrReplaceTempView(s"tblClusters$appId")
     
      spark.sql(s"""drop table if exists tblClusters_pq$appId""")
      spark.sql(s"""create table tblClusters_pq$appId stored as parquet 
        as
        select 
          prediction eid,
          rowid mid,
          snpi, 
          splname, 
          spfname 
        from tblClusters$appId""")
      
      var WSSSE = 0.0
      var cluctrlen = 0
      //val kmModel  = pipeModel.stages(5).asInstanceOf[[Bisecting]KMeansModel]
      val kmeanIdx = useDCT match { 
        case true => 5
        case false => 4
      }
      
      pipeModel.stages(kmeanIdx) match {
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
          
      predictionResult

  }
}