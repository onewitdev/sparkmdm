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
import scala.util.control.Breaks._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Cleanup {


  def log = Logger.getLogger(Cleanup.getClass())
      log.setLevel(Level.DEBUG)
      
  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }

  def rmtblist (spark : SparkSession, tblist : Array[Row]) : Any = {
   try {
    
     //for (r <- tblist) spark.sql("drop table if exists "+r(0))  
     tblist foreach {r => 
       if ((""+r(1)).toBoolean) //temp table
         scala.util.Try(spark.sqlContext.dropTempTable(""+r(0)))
       else
         spark.sql("drop table if exists "+r(0))
       log.info("table "+r(0)+ " deleted.")
     }
   } 
   catch {case e: Exception => None}
  }

    
  def rm (spark : SparkSession, appId : String) : Any = {

    var tables = spark.sql("show tables")
      .filter(s"""tableName like '%$appId'""")
      //.filter($"tableName" like s"'%$appId'")
      .filter("not(tableName like 'inputmbrs_eids_pq%')")
      .filter("not(tableName like 'tblEntity_pq%')")

    rmtblist(spark,tables.collect())
  }

  def rmall (spark : SparkSession) = {
    var tables = spark.sql("show tables")
    rmtblist(spark,tables.collect())   
  }


  def rmOld (spark : SparkSession, appId : String) = {
    
    //1. Tables used in LoadNPIData
    spark.sql(s"DROP TABLE If EXISTS inputmbrs$appId")

    spark.sql(s"""drop table if exists  inputmbrs_pq$appId""") //$appId

    //2. Tables used in ValueFreq
    //spark.sql(s"""drop table if exists tblValueFreq$appId""") 
    
    //3. Dedupe
    spark.sql(s"""drop table if exists  tblDuplicates$appId""")
    spark.sql(s"""drop table if exists  duplicates_pq$appId""")
    spark.sql(s"""drop table if exists uinputmbrs_pq$appId""")

    //4. Bucketing
    spark.sql(s"""drop table if exists tblBuckets$appId""")
    
    //5. Comparison Pairs
    //spark.sql(s"UNCACHE TABLE if exists tblAllPairs$appId")
    spark.sql(s"DROP TABLE if exists tblAllPairs$appId")
    
    //6. Pairs Scoring
    //spark.sql(s"UNCACHE TABLE tblAllScores$appId")
    spark.sql(s"DROP TABLE if exists tblAllScores$appId")
    
    //7. Threshold
    //spark.sql(s"UNCACHE TABLE tblThresholds$appId") 
    spark.sql(s"DROP TABLE if exists tblThresholds$appId") 
    
    //8. Binary Classification
    //spark.sql(s"""drop table if exists tblmatching$appId""")
    spark.sql(s"""drop table if exists tbllinking$appId""")

    spark.sql(s"""drop table if exists tblvaluefreq$appId""")   
    spark.sql(s"""drop table if exists tmptblvaluefreq$appId""")
    //8.MR
    spark.sql(s"""drop table if exists tblEMPairs_pq$appId""")
    
    //9.EidLinkBack
    spark.sql(s"DROP TABLE if exists tblMemEids$appId") 
    
  }
}