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
import org.apache.spark.storage._


object InputDF2Table {
  
  def temptable (spark : SparkSession, appId : String,config: MDMConfig, df: DataFrame) = {
    
       df.createOrReplaceTempView(s"inputmbrs$appId")
       
       config.strategy match {
         /*
        case ResourceStrategy.None => None
        case ResourceStrategy.Cache => 
          df.persist(StorageLevel.MEMORY_AND_DISK)
        case ResourceStrategy.LazyTable => 
          spark.sql(s"CACHE LAZY TABLE inputmbrs$appId")
        case ResourceStrategy.Table => 
          spark.sql(s"CACHE TABLE inputmbrs$appId")
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
        
        */
         case _ => { //regardless ResourceStartegy, we need to save the input to disk in HDFS Parquet file
          //We save the randomized input data to HDFS as Parquet Columnar Format 
          //so that when we finished the whole process, we can link back the eids against rowids
          spark.sql(s"""drop table if exists inputmbrs_pq$appId""") //$appId
          spark.sql(s"""create table inputmbrs_pq$appId stored as parquet as select * from inputmbrs$appId""") //$appId
          //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
          scala.util.Try(spark.sqlContext.dropTempTable(s"inputmbrs$appId"))
        } //end case
      } //end match
         
    
  }
}