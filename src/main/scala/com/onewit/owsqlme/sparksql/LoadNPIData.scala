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
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object LoadNPIData {

  
  val range = 0 to 10
  val rnd1 = new scala.util.Random
  val rnd2 = new scala.util.Random
  val rnd3 = new scala.util.Random

  def rndString1(inStr: String): String = {
  	var r = inStr
  	val ri = rnd1.nextInt(range length) 
  	if (ri<3 && inStr.length>0) r = inStr + inStr.last
  	r
  }

  def rndString2(inStr: String): String = {
  	var r = inStr
  	val ri = rnd2.nextInt(range length) 
  	if (ri<3 && inStr.length>0) r = inStr + inStr.last
  	r
  }

  def rndString3(inStr: String): String = {
  	var r = inStr
  	val ri = rnd3.nextInt(range length) 
  	if (ri<3 && inStr.length>0) r = inStr + inStr.last
  	r
  }

  def load (spark : SparkSession, file : String, st: StructType, appId : String ) = {

    val sc = spark.sparkContext
    

    val npis = sc.textFile(file)
    var arrnpis=  npis.map(_.split("\\|"))
    
    
    var rownpis = arrnpis.map(la => Array(
    la(0), la(1) , la(2) , la(3) , la(4) , la(5) , la(6) , la(7) , la(8) , la(9) , 
    la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19) /*, 
    la(20), la(21) , la(22) , la(23) , la(24) , la(25) , la(26) , la(27) , la(28) , la(29) ,
    la(30), la(31) , la(32) , la(33) , la(34) , la(35) , la(36) , la(37) , la(38) , la(39) ,
    la(40), la(41) , la(42) , la(43) , la(44) , la(45) , la(46) , la(47) , la(48) , la(49) */
    ))
    
    
    
    var rownpis2 = sc.union(rownpis,rownpis)
    
    var rownpis4 = sc.union(rownpis2.map(la => Array(
    rndString1(la(0)),la(1) , la(2) , la(3) , la(4) , rndString2(la(5)),rndString3(la(6)),la(7) , la(8) , la(9) , 
    la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19) /*, 
    la(20), la(21) , la(22) , la(23) , la(24) , la(25) , la(26) , la(27) , la(28) , la(29) ,
    la(30), la(31) , la(32) , la(33) , la(34) , la(35) , la(36) , la(37) , la(38) , la(39) ,
    la(40), la(41) , la(42) , la(43) , la(44) , la(45) , la(46) , la(47) , la(48) , la(49) */
    ))
    ,rownpis2)
    
    var rownpis8 = sc.union(rownpis4.map(la => Array(
    rndString1(la(0)),la(1) , la(2) , la(3) , la(4) , rndString2(la(5)),rndString3(la(6)),la(7) , la(8) , la(9) , 
    la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19) /*, 
    la(20), la(21) , la(22) , la(23) , la(24) , la(25) , la(26) , la(27) , la(28) , la(29) ,
    la(30), la(31) , la(32) , la(33) , la(34) , la(35) , la(36) , la(37) , la(38) , la(39) ,
    la(40), la(41) , la(42) , la(43) , la(44) , la(45) , la(46) , la(47) , la(48) , la(49) */
    ))
    ,rownpis4)
    var rownpis10 = sc.union(rownpis8,rownpis2)
    
    /*
    var rownpis1 = rownpis10.map(la => Row(
    la(0) , la(1) , la(2) , la(3) , la(4) , la(5), la(6), la(7), la(8) , la(9) , 
    la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19)  , 
    la(20) , la(21) , la(22) , la(23) , la(24) , la(25) , la(26) , la(27) , la(28) , la(29) ,
    la(30) , la(31) , la(32) , la(33) , la(34) , la(35) , la(36) , la(37) , la(38) , la(39) ,
    la(40) , la(41) , la(42) , la(43) , la(44) , la(45) , la(46) , la(47) , la(48) , la(49) 
    ))
    */
    
    /*
    var rownpis1 = rownpis10.zipWithIndex.map{case (la, ln) => Row(
    ln.toInt + 1,
    la(0) , la(1) , la(2) , la(3) , la(4) , la(5), la(6), la(7), la(8) , la(9) , 
    la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19)/*,
    la(20) , la(21) , la(22) , la(23) , la(24) , la(25) , la(26) , la(27) , la(28) , la(29) ,
    la(30) , la(31) , la(32) , la(33) , la(34) , la(35) , la(36) , la(37) , la(38) , la(39) ,
    la(40) , la(41) , la(42) , la(43) , la(44) , la(45) , la(46) , la(47) , la(48) , la(49) */ 
    )}
    */
    
    /*
    val npidata6DF = spark.sql("SELECT * FROM npidata7 where 1<>1")
    val schema = npidata6DF.schema
    */
    
 
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
    
    var rownpis1 = rownpis10.zipWithIndex.map{case (la, ln) => Row(
    ln.toInt + 1,
    la(0) , la(1) , la(2) , la(3) , la(4) , la(5), la(6), la(7), la(8) , la(9) , 
    la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19)/*,
    la(20) , la(21) , la(22) , la(23) , la(24) , la(25) , la(26) , la(27) , la(28) , la(29) ,
    la(30) , la(31) , la(32) , la(33) , la(34) , la(35) , la(36) , la(37) , la(38) , la(39) ,
    la(40) , la(41) , la(42) , la(43) , la(44) , la(45) , la(46) , la(47) , la(48) , la(49) */ 
    )}

    val df_npis1 = spark.createDataFrame(rownpis1, schema)  
    //val df_npis1 = rownpis1.toDF()
    df_npis1.createOrReplaceTempView(s"inputmbrs$appId")
    spark.sql(s"CACHE LAZY TABLE inputmbrs$appId")
    

    
    //We save the randomized input data to HDFS as Parquet Columnar Format 
    //so that when we finished the whole process, we can link back the eids against rowids
    spark.sql(s"""drop table if exists  inputmbrs_pq$appId""") //$appId
    spark.sql(s"""create table inputmbrs_pq$appId stored as parquet as select * from inputmbrs$appId""") //$appId

  }
}