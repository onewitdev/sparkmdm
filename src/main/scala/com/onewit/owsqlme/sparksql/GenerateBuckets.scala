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
import com.onewit.owsqlme.udaf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._



object GenerateBuckets {
  
   
  def genbuckets (spark : SparkSession,appId : String,config: MDMConfig) = {
        
    HashingUDFs.register(spark)

    var tblsuffixI = appId
    //config.strategy match {
    //  case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
        tblsuffixI = "_pq"+appId
    //  }
    //  case _ => tblsuffixI = appId
    //}
    //uinputmbrs$appId joined twice gives a strange rowid not found error
    var hashedSQL = s"""
      select RowID, 1 as BktId, 
      md5(soundex(CONCAT(substring(plname,1,3),"_",substring(pfname,1,3)))) bktHash
      from uinputmbrs$tblsuffixI
    """  
    /*
    union all
    select RowID, 2 as BktId, 
    md5(soundex(CONCAT(substring(npi,1,5),"_",substring(pfname,1,3)))) bktHash
    from uinputmbrs$tblsuffix
    union all
    select RowID, 3 as BktId, 
    md5(soundex(CONCAT(substring(npi,1,5),"_",substring(plname,1,3)))) bktHash
    from uinputmbrs$tblsuffix
    */
    var hb = spark.sql(hashedSQL) //.show()         
    hb.createOrReplaceTempView(s"tblBuckets$appId")
    
    var tblsuffixO = appId
    config.strategy match {
      case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
        tblsuffixO = "_pq"+appId
      }
      case _ => tblsuffixO = appId
    }   
    config.strategy match {
      case ResourceStrategy.None => None
      case ResourceStrategy.Cache => 
        hb.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE tblBuckets$appId")
      case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => 
        spark.sql(s"CACHE TABLE tblBuckets$appId")
      case ResourceStrategy.Disk => {
        spark.sql(s"""drop table if exists tblBuckets$tblsuffixO""")
        spark.sql(s"""create table tblBuckets$tblsuffixO stored as parquet as select * from tblBuckets$appId""")
        //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
        scala.util.Try(spark.sqlContext.dropTempTable(s"tblBuckets$appId"))
        hb.unpersist()
      } //end case
    } //end match   
    
  }
  
}