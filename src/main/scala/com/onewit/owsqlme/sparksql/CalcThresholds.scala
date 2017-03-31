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

object CalcThresholds {
    
  def thresholds (spark : SparkSession,appId : String,config: MDMConfig) = {
    
    var tblsuffix = appId
    config.strategy match {
      case ResourceStrategy.Disk => {
        tblsuffix = "_pq"+appId
      }
      case _ => tblsuffix = appId
    }     

    //Use distinct value frequency as weight for threshold calculation
    //Hardcoded thresholds for demo purposes
    var thresholdSQL = s"""
      with avs as (
        select ValType,log2(count(distinct value))*100 as W_DVF 
        from tblValueFreq$tblsuffix group by ValType
      )
      select 
      0.5*SUM(W_DVF) as Autolink,
      0.35*SUM(W_DVF) as ClericReview
      from avs
  	  """
  
  	var thresholds = spark.sql(thresholdSQL)
    thresholds.createOrReplaceTempView(s"tblThresholds$appId") 
    config.strategy match {
      case ResourceStrategy.None => None
      case ResourceStrategy.Cache => 
        thresholds.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE tblThresholds$appId")
      case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk=> 
        spark.sql(s"CACHE TABLE tblThresholds$appId")
      case ResourceStrategy.Disk => {
        spark.sql(s"""drop table if exists tblThresholds$tblsuffix""") //$appId
        spark.sql(s"""create table tblThresholds$tblsuffix stored as parquet as select * from tblThresholds$appId""") //$appId
        //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
        scala.util.Try(spark.sqlContext.dropTempTable(s"tblThresholds$appId"))
        thresholds.unpersist()
      } //end case
    } //end match   

    config.strategy match {
        case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => {
          try {
               spark.sql(s"UNCACHE TABLE tblValueFreq$appId")
          } catch { case _ : Throwable =>  None }   
          try {
               spark.sqlContext.dropTempTable(s"tblValueFreq$appId")
          } catch { case _ : Throwable =>  None }  
        }
        case ResourceStrategy.Disk => {  
          try {
               spark.sqlContext.dropTempTable(s"tblValueFreq$appId")
          } catch { case _ : Throwable =>  None }  
        }
        
        case _ => None       
      }
 
   
  }
}