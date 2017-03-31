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

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import com.onewit.owsqlme.config._
import com.onewit.owsqlme.config._

object LoadDataExpNRnd {


  val range = 0 to 10
  val rnd = new scala.util.Random


  def rndString(inStr: String): String = {
  	var r = inStr
  	val ri = rnd.nextInt(range length) 
  	if (ri<3 && inStr.length>0) r = inStr + inStr.last
  	r
  }

  def load (spark : SparkSession, file : String, st: StructType, appId : String , config: MDMConfig) = {
    
    val npis = spark.sparkContext.textFile(file)
    var arrnpis=  npis.map(_.split("\\|"))
    
    //Taking each line from input, plus indexed rowid, 
    //we create 10 lines with some values randomly suffixed with last char
    var rndnpis = arrnpis.zipWithIndex.flatMap{case (la, ln) => 
       for(i<-0 to 9) yield { 
         Row(ln.toInt*10 + i,
         rndString(la(0)),la(1) , la(2) , la(3) , la(4) , rndString(la(5)),rndString(la(6)),la(7) , la(8) , la(9) , 
         la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19)
         )
       }
    }
    
    val schema = StructType(
      Array(
        StructField("rowid",IntegerType,true), 
        StructField("npi",StringType,true), 
        StructField("typecode",StringType,true), 
        StructField("rnpi",StringType,true), 
        StructField("ein",StringType,true), 
        StructField("porg",StringType,true), 
        StructField("plname",StringType,true), 
        StructField("pfname",StringType,true), 
        StructField("pmname",StringType,true), 
        StructField("pprefix",StringType,true), 
        StructField("psuffix",StringType,true), 
        StructField("pcred",StringType,true), 
        StructField("poorgname",StringType,true), 
        StructField("potypecode",StringType,true), 
        StructField("polname",StringType,true), 
        StructField("pofname",StringType,true), 
        StructField("pomname",StringType,true), 
        StructField("ponameprefix",StringType,true), 
        StructField("ponamesuffix",StringType,true), 
        StructField("pocred",StringType,true)
      )
    )
        
    case class Member (
      rowid: Integer,
      npi: String,
      typecode: String,
      rnpi: String,
      ein: String,
      porg: String,
      plname: String,
      pfname: String,
      pmname: String,
      pprefix: String,
      psuffix: String,
      pcred: String,
      poorgname: String,
      potypecode: String,
      polname: String,
      pofname: String,
      pomname: String,
      ponameprefix: String,
      ponamesuffix: String,
      pocred: String,
      onemore: String
    )

    val df_npis1 = spark.createDataFrame(rndnpis, schema)  
    //val df_npis1 = rndnpis.toDF()
    df_npis1.createOrReplaceTempView(s"inputmbrs$appId")
    
    config.strategy match {
      case ResourceStrategy.None => None
      case ResourceStrategy.Cache => 
        df_npis1.persist(StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE inputmbrs$appId")
      case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
        //We save the randomized input data to HDFS as Parquet Columnar Format 
        //so that when we finished the whole process, we can link back the eids against rowids
        spark.sql(s"""drop table if exists inputmbrs_pq$appId""") //$appId
        spark.sql(s"""create table inputmbrs_pq$appId stored as parquet as select * from inputmbrs$appId""") //$appId
        //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
        scala.util.Try(spark.sqlContext.dropTempTable(s"inputmbrs$appId"))
      } //end case
    } //end match

    
  } //end def load
}