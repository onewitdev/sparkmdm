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

import org.apache.log4j.Logger
import org.apache.log4j.Level

import com.onewit.owsqlme.config._
import com.onewit.owsqlme.util._
import com.onewit.owsqlme.udaf._

object CalcPairSimScores {
   
   def genscores (spark : SparkSession,appId : String,config: MDMConfig) = {
     
      NGCompareUDFs.register(spark)
 
      var log =Logger.getLogger(this.getClass())
            
      var tblsuffixI = appId
      config.strategy match {
        case ResourceStrategy.Disk  => {
          tblsuffixI = "_pq"+appId
        }
        case _ => tblsuffixI = appId
      }
      
      var tblsuffixI2 = "_pq"+appId

      var ct = s"["+new TimeUtils.CurrentTime().ctstring+"]"
      var logMsg=s"\r\n${ct}\t\tPreparing Comparison Pair Attributes...\r\n"
      log.info(logMsg)
      printf(logMsg)
      var pairAttrsSQL = s"""
        select p1.rowid as mid1, 
        p2.rowid as mid2,
        p1.npi as npi1, 
        p2.npi as npi2,

        p1.plname plname1, 
        p2.plname plname2,
        
        p1.pfname pfname1, 
        p2.pfname pfname2 
        from       
        tblAllPairs$tblsuffixI p
      	inner join uinputmbrs$tblsuffixI2 p1 on p.mid1 = p1.rowid 
      	inner join uinputmbrs$tblsuffixI2 p2 on p.mid2 = p2.rowid
      	DISTRIBUTE BY mid2      	
      """
      var pairAttrs = spark.sql(pairAttrsSQL)               
      pairAttrs.createOrReplaceTempView(s"tblAllPairAttrs$appId") 
      //spark.sql(s"CACHE TABLE tblAllPairAttrs$appId")

      config.strategy match {
        case ResourceStrategy.None => None
        case ResourceStrategy.Cache => {
          pairAttrs.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        }
        case ResourceStrategy.LazyTable => 
          spark.sql(s"CACHE LAZY TABLE tblAllPairAttrs$appId")
        case ResourceStrategy.Table  => 
          spark.sql(s"CACHE TABLE tblAllPairAttrs$appId")
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk=> {
          spark.sql(s"""drop table if exists tblAllPairAttrs_pq$appId""") //$appId
          spark.sql(s"""create table tblAllPairAttrs_pq$appId stored as parquet 
            as select * from tblAllPairAttrs$appId""") //$appId
          spark.sqlContext.dropTempTable(s"tblAllPairAttrs$appId")
        } //end case
      } //end match    
          
      logMsg=s"\r\n${ct}\t\tScoring Comparison Pairs on MDM Attributes...\r\n"
      log.info(logMsg)
      printf(logMsg)

      var tblsuffixItmp = appId
      config.strategy match {
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk  => {
          tblsuffixItmp = "_pq"+appId
        }
        case _ => tblsuffixItmp = appId
      }
      var scoringSQL = s"""
        with scores as (
          select 
          mid1, 
          mid2,
          ngcompare(p.npi1, p.npi2) sc_npi,
          ngcompare(p.plname1, p.plname2) sc_ln,
          ngcompare(p.pfname1, p.pfname2) sc_fn
          from       
          tblAllPairAttrs$tblsuffixItmp p --tmptblPairAttrs$appId p
        )
        select 
          mid1, 
          mid2,
          sc_npi,
          sc_ln,
          sc_fn,
          (sc_npi + sc_ln + sc_fn) as score
        from scores
        DISTRIBUTE BY mid2
      """


      var scores = spark.sql(scoringSQL)               
      scores.createOrReplaceTempView(s"tblAllScores$appId") 
      
      var tblsuffixO = "_pq"+appId
      config.strategy match {
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk=> {
          tblsuffixO = "_pq"+appId
        }
        case _ => tblsuffixO = appId
      }         
      config.strategy match {
        case ResourceStrategy.None => None
        case ResourceStrategy.Cache => {
          scores.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        }
        case ResourceStrategy.LazyTable => 
          spark.sql(s"CACHE LAZY TABLE tblAllScores$appId")
        case ResourceStrategy.Table  => 
          spark.sql(s"CACHE TABLE tblAllScores$appId")
          spark.sql(s"UNCACHE TABLE tblAllPairAttrs$appId")
          try {
           spark.sqlContext.dropTempTable(s"tblAllPairAttrs$appId")
          } catch { case _ : Throwable =>  None }  

        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk=> {
          spark.sql(s"""drop table if exists tblAllScores$tblsuffixO""") //$appId
          spark.sql(s"""create table tblAllScores$tblsuffixO stored as parquet as select * from tblAllScores$appId""") //$appId    
          try {
           spark.sqlContext.dropTempTable(s"tblAllPairAttrs$appId")
          } catch { case _ : Throwable =>  None }  
      
        } //end case
      } //end match    

      //Previously Cached Temp Table Removal
      config.strategy match {
        case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => {
          try {
               spark.sql(s"UNCACHE TABLE tblAllPairs$appId")
          } catch { case _ : Throwable =>  None }
          try {
           spark.sqlContext.dropTempTable(s"tblAllPairs$appId")
          } catch { case _ : Throwable =>  None }  
        }
        case ResourceStrategy.Disk => {
          try {
           spark.sqlContext.dropTempTable(s"tblAllPairs$appId")
          } catch { case _ : Throwable =>  None }  
        }        
        case _ => None       
      }  
       
  }  
}