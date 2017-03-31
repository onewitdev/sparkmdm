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

object FindMatchPairs {
  
  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }
  
  def matchpairs (spark : SparkSession,appId : String,config: MDMConfig) = {
     

      var tblsuffixI = appId
      config.strategy match {
        case ResourceStrategy.Disk => {
          tblsuffixI = "_pq"+appId
        }
        case _ => tblsuffixI = appId
      }
      
      var tblsuffixII = appId
      config.strategy match {
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
          tblsuffixII = "_pq"+appId
        }
        case _ => tblsuffixII = appId
      }
      
      var mpSQL = s"""
        SELECT distinct mid1, mid2 
        FROM tblAllScores$tblsuffixII s
        WHERE s.score >= (
          select 
        	Autolink
        	from tblThresholds$tblsuffixI
        )
      """
      var mps=spark.sql(mpSQL)
      mps.createOrReplaceTempView(s"tblMatchPairs$appId") 

 
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
          mps.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        case ResourceStrategy.LazyTable => 
          spark.sql(s"CACHE LAZY TABLE tblMatchPairs$appId")
        case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => 
          spark.sql(s"CACHE TABLE tblMatchPairs$appId")
        case ResourceStrategy.Disk => {
          spark.sql(s"""drop table if exists tblMatchPairs$tblsuffixO""") //$appId
          spark.sql(s"""create table tblMatchPairs$tblsuffixO stored as parquet as select * from tblMatchPairs$appId""") //$appId
        } //end case
      } //end match          

            //Previously Cached Temp Table Removal
      config.strategy match {
        case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk | ResourceStrategy.Disk=> {
          try {
               spark.sql(s"UNCACHE TABLE tblThresholds$appId")
          } catch { case _ : Throwable =>  None }   
          try {
               spark.sql(s"UNCACHE TABLE tblAllScores$appId")
          } catch { case _ : Throwable =>  None }   
          try {
               spark.sqlContext.dropTempTable(s"tblThresholds$appId")
          } catch { case _ : Throwable =>  None }  
          try {
               spark.sqlContext.dropTempTable(s"tblAllScores$appId")
          } catch { case _ : Throwable =>  None }  
        }
        case _ => None       
      }  

  }
}