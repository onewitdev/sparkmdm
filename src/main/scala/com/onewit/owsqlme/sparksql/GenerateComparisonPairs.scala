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

object GenerateComparisonPairs {
   
   def genpairs (spark : SparkSession,appId : String,config: MDMConfig) = {
    
      var tblsuffixI = appId
      config.strategy match {
        case ResourceStrategy.Disk => {
          tblsuffixI = "_pq"+appId
        }
        case _ => tblsuffixI = appId
      } 
     
      /* we can exclude rows that are only in a single bucket to reduce comparison pairs further
       * 
       */
      var pairingSQL = s"""
      	with grpedbuckets as (
      		SELECT dense_rank() over (partition by bktid order by bkthash) as grpid,
      		rowid as mid
      		FROM tblBuckets$tblsuffixI
      		where bkthash is not null
      	)
      	, cntedgrpedbuckets as (
      		select 
      		grpid, count(*) as memcnt
      		from grpedbuckets
      		group by grpid
      		having count(*) < 3000 
      	)
      	select bk2.mid as mid1 ,bk1.mid as mid2
      	from grpedbuckets bk1, 
      	grpedbuckets bk2,
      	cntedgrpedbuckets g
      	where bk1.grpid = bk2.grpid and g.grpid = bk1.grpid
      	and bk1.mid < bk2.mid
      	and g.memcnt < 3000 
      """
      var pairs = spark.sql(pairingSQL) //.show()
      pairs.createOrReplaceTempView(s"tblAllPairs$appId") 

      
      var tblsuffixO = appId
      config.strategy match {
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
          tblsuffixO = "_pq"+appId
        }
        case _ => tblsuffixO = appId
      } 
      
      //spark.sql("select count(*) from tblAllPairs").show()
      config.strategy match {
        case ResourceStrategy.None => None
        case ResourceStrategy.Cache => 
          pairs.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        //case ResourceStrategy.LazyTable  => 
        //  spark.sql(s"CACHE LAZY TABLE tblAllPairs$appId")
        //commenting out the 2 lines above fixes this exception:
        //  java.lang.IllegalArgumentException: 
        //requirement failed: PartitioningCollection requires all of its partitionings 
        //have the same numPartitions.
        case ResourceStrategy.Table | ResourceStrategy.LazyTable => 
          spark.sql(s"CACHE TABLE tblAllPairs$appId")
        case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
          spark.sql(s"""drop table if exists tblAllPairs$tblsuffixO""")
          spark.sql(s"""create table tblAllPairs$tblsuffixO stored as parquet as select * from tblAllPairs$appId""")
        } //end case
      } //end match 
      
      //Previously Cached Temp Table Removal
      config.strategy match {
        case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk | ResourceStrategy.MemoryAndDisk=> {
          try {
              spark.sql(s"UNCACHE TABLE tblBuckets$appId")
          } catch { case _ : Throwable =>  None }
          try {
               spark.sqlContext.dropTempTable(s"tblBuckets$appId")
          } catch { case _ : Throwable =>  None }       
        }
        case _ => None       
      }
      
  }  
}