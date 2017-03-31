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

import scala.util.control.Breaks._
import com.onewit.owsqlme.util._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object MatchReduce {
  
  def USE_DENSE_RANK_P1 : Boolean = true
  
  def mr (spark : SparkSession, appId : String,config: MDMConfig) = {

      var tblsuffix = appId
      config.strategy match {
        case ResourceStrategy.Disk => {
          tblsuffix = "_pq"+appId
        }
        case _ => tblsuffix = appId
      }   

      var log =Logger.getLogger(MatchReduce.getClass())
    
      /* in Spark SQL, due to the distributed nature, dense_rank() over all rows causes shuffle dependency
       * on all previous data blocks(buckets) - all partitions have to be shuffled to 1 
       * In order to avoid this, rdd.zipWithIndex or monotonicallyIncreasingId are 2 alternatives.
       * 
       * Unfortunately, monotonicallyIncreasingId consumes too much memory, preventing this app to run successfully with 8G memory
       * so we revert back to using dense_rank() over (partition by 1), namely USE_DENSE_RANK_P1
       * 
       * The attempted code is kept so we can switch to using it in a production cluster with a lot of memory
       */ 
      var mrSQL = ""
      if (USE_DENSE_RANK_P1) {
       	mrSQL = s"""
      	SELECT dense_rank() over (partition by 1 order by mid2) as eid,
      	mid1,mid2 FROM tblMatchPairs$tblsuffix
      	DISTRIBUTE BY eid      
      	"""
      }
      else {
        var mid2SQL = s"""
        	SELECT distinct mid2 FROM tblMatchPairs$tblsuffix
        	ORDER BY mid2
        """
        /*val mid2DF = spark.sql(mid2SQL)
        val mid2EidSchema = mid2DF.schema.add(StructField("eid",IntegerType,true))
        val mid2Eidrdd = mid2DF.rdd.zipWithIndex.map{case (la, ln) => Row(la(0),ln.toInt+1)}
        val mid2EidDF = spark.createDataFrame(mid2Eidrdd, mid2EidSchema) 
        */
        val mid2EidDF = spark.sql(mid2SQL).withColumn("eid", monotonicallyIncreasingId) //MonotonicallyIncreasingId is obsolete?    
        mid2EidDF.createOrReplaceTempView(s"tblMid2Eids$appId") 
  
        mrSQL = s"""
        	SELECT m2eid.eid,
        	mp.mid1,mp.mid2 FROM tblMatchPairs$tblsuffix mp, tblMid2Eids$appId m2eid
        	where mp.mid2 = m2eid.mid2
        	DISTRIBUTE BY mid2
        """
    	}	
      
      var mrs=spark.sql(mrSQL)
      mrs.createOrReplaceTempView(s"tblMPairEids$appId") 
      spark.sql(s"CACHE LAZY TABLE tblMPairEids$appId")
	    //mrs.persist(StorageLevel.MEMORY_AND_DISK_ONLY)

     
      var empSQL=s"""
    	--SET spark.sql.shuffle.partitions=200;
    	with pairs as (
    	select distinct eid, mid2 as mid from tblMPairEids$appId
    	union all 
    	select distinct eid,mid1 as mid from tblMPairEids$appId
    	)
    	select eid,mid from pairs DISTRIBUTE BY mid
      """
      
      var emp = spark.sql(empSQL)
      //emp.cache()
      emp.createOrReplaceTempView(s"tblEMPairs0$appId")  //cannot use _ later in $loopCnt$appId situation
      spark.sql(s"CACHE LAZY TABLE tblEMPairs0$appId")
      //spark.cacheTable("tblEMPairs0")
      //spark.uncacheTable("tblEMPairs0")
      
      spark.sql(s"UNCACHE TABLE tblMPairEids$appId")
      spark.sql(s"DROP TABLE tblMPairEids$appId")
   

      var loopCnt:Int = 0 //Needs to start from 0 due to previous table name convention!!
      var needMRSQL = s"""
    	select 1 g1 
      from tblEMPairs$loopCnt$appId
    	--where 1<>1
    	group by mid having count(distinct eid)>1 
      """
      var needMR = spark.sql(needMRSQL).limit(1).take(1) //first() //
	
      var ct = s"["+new TimeUtils.CurrentTime().ctstring+"]"
      var logMsg=s"\r\n${ct}\t\tAny Need for Match-Reduce?...\r\n"
      log.info(logMsg)
      printf(logMsg)

      while (!needMR.isEmpty && loopCnt<4 //hardcoded to 4 to save memory;normally take 4-5 iterations
          ) {
      	ct = s"["+new TimeUtils.CurrentTime().ctstring+"]"
      	logMsg=s"\r\n${ct}\t\t%%=-> Match-Reduce Depth : " + (loopCnt+1) + "...\r\n"
      	log.info(logMsg)
      	printf(logMsg)
      	var nextLoopCnt:Int = loopCnt+1
        var mreduceSQL = s"""
      		with part_sorted_grps as (
      			-- rowid=1 row's eid in each eid partition is the to-eid that a given mid's all other eids should reduce to!
      			SELECT 
      				ROW_NUMBER() over (partition by mid order by eid asc) rid, 
      				--COUNT() over (partition by mid) mcnt,
      				mid, 
      				eid
      			FROM tblEMPairs$loopCnt$appId
      		)
      		, eidSetToEid as (
      			select 
      				--p1.rid,
      				p1.mid,
      				p1.eid eid, 
      				p2.eid toeid
      			from 
      				part_sorted_grps p1, 
      				part_sorted_grps p2
      			where p1.mid = p2.mid 
      				and p1.rid > p2.rid 
      				and p2.rid=1
      				
      				--and p1.mcnt > 1 and p2.mcnt > 1
      		)
      		,eidSetToMinEid as (
      			select eid, min(toeid) as toeid 
      			from eidSetToEid where eid<>toeid
      			group by eid
      		)
      		,reduced as (
      			select distinct meid.toeid as eid , m.mid
      			from 
      			eidSetToMinEid meid --,tblEMPairs$loopCnt$appId m
      			inner join tblEMPairs$loopCnt$appId m
      			on m.eid = meid.eid and m.eid <> meid.toeid
      			--if an eid is the mineid others are going to be set to in one mid group
      			--but in another group, its going to be set to something else, we should defer
      			--the set in the other group
      			left outer join eidSetToMinEid meid2 
      			on m.eid = meid2.toeid 
      			where --m.eid = meid.eid AND 
      			--m.eid <> meid.toeid AND
      			meid2.toeid IS NULL
      		
      			--- now add the eid-mid pairs that were single member groups
      			union all
      			select eid, mid from part_sorted_grps
      			where rid=1

      		)
      		select eid,mid 
      		from reduced 		--deduped
      		group by eid,mid 
        """ 
  
        var mreduce = spark.sql(mreduceSQL)	
        mreduce.createOrReplaceTempView(s"tblEMPairs$nextLoopCnt$appId") //THIS IS COLL!!!
        //printf(s"CACHE LAZY TABLE tblEMPairs$nextLoopCnt$appId ...\r\n") 
        spark.sql(s"CACHE TABLE tblEMPairs$nextLoopCnt$appId")  //LAZY 
        //mreduce.cache()
        spark.sql(s"UNCACHE TABLE tblEMPairs$loopCnt$appId") //get ride of the old table cache
        spark.sql(s"DROP TABLE tblEMPairs$loopCnt$appId") //get ride of the old table cache
        
        
        needMRSQL = s"""
        select 1 g1 
      	from tblEMPairs$nextLoopCnt$appId
        --where 1<>1
      	group by mid having count(distinct eid)>1 
      	"""
	      ct = s"["+new TimeUtils.CurrentTime().ctstring+"]"
        logMsg=s"\r\n${ct}\t\tAny Need for more Match-Reduce?...\r\n"
        log.info(logMsg)
        printf(logMsg)      
        needMR = spark.sql(needMRSQL).limit(1).take(1)
        //printf("Need another MR?...\r\n") 
        //emp.unpersist()
        //emp = mreduce
        
        loopCnt = loopCnt  + 1
      
      }
      //printf(s"Match-Reduce Done. Final table is tblEMPairs$loopCnt$appId" )
      
      //The eids in the last table are not sequential due to matched eids reduce to the min value 
      //we can resequence it here. Remember, when tblEntity participates,
      //we need to rework the logic
 
      var finalEMSQL = ""
      if (USE_DENSE_RANK_P1) {
        finalEMSQL =s"""
      			SELECT 
      				DENSE_RANK() over (partition by 1 order by eid asc) as eid,
      				mid
      			FROM tblEMPairs$loopCnt$appId
        """
      }
      else {
        val reseqEMSQL = s"""
      			SELECT distinct eid as oldeid
      			FROM tblEMPairs$loopCnt$appId
      			ORDER BY eid
        """
        val eidODF = spark.sql(reseqEMSQL).withColumn("eid", monotonicallyIncreasingId) //MonotonicallyIncreasingId is obsolete?    
        eidODF.createOrReplaceTempView(s"tblEidO2Eids$appId")    
        
        //val 
        finalEMSQL = s"""
      			SELECT eo2e.eid,em1.mid
      			FROM tblEMPairs$loopCnt$appId em1, tblEidO2Eids$appId eo2e
      			where em1.eid = eo2e.oldeid
        """   
      }
      val finalEM = spark.sql(finalEMSQL)
      finalEM.createOrReplaceTempView(s"tblEMPairs$appId")

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
          finalEM.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        case ResourceStrategy.LazyTable  => 
          spark.sql(s"CACHE LAZY TABLE tblEMPairs$appId")
        case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => 
          spark.sql(s"CACHE TABLE tblEMPairs$appId")
        case ResourceStrategy.Disk  => {
          spark.sql(s"""drop table if exists tblEMPairs$tblsuffixO""") //$appId
          spark.sql(s"""create table tblEMPairs$tblsuffixO stored as parquet as select * from tblEMPairs$appId""") //$appId
          //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
          scala.util.Try(spark.sqlContext.dropTempTable(s"tblEMPairs$appId"))
          finalEM.unpersist()
        } //end case
      } //end match          
      spark.sql(s"DROP TABLE tblEMPairs$loopCnt$appId") //get ride of the last table cache

      //Previously Cached Temp Table Removal
      config.strategy match {
        case ResourceStrategy.Table |  ResourceStrategy.MemoryAndDisk => {
          
          try {
            spark.sql(s"UNCACHE TABLE tblMatchPairs$appId")
          } catch { case _ : Throwable =>  None }  
          try {
            spark.sqlContext.dropTempTable(s"tblMatchPairs$appId")
          } catch { case _ : Throwable =>  None }  
        }
        case ResourceStrategy.Disk => {
          try {
            spark.sqlContext.dropTempTable(s"tblMatchPairs$appId")
          } catch { case _ : Throwable =>  None }  
        }
        case _ => None       
      }    
  }
}