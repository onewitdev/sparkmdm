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
import java.security.MessageDigest  
import com.rockymadden.stringmetric.phonetic._

//import org.apache.commons.codec.language.Soundex

object Dedupe {
  
  def dedupe (spark : SparkSession,appId : String,config: MDMConfig) = {
        
    //Part 1. Produce tblDuplicates table to store duplicates that are not to participate matching
    var tblsuffixI = appId
    config.strategy match {
      case _ => tblsuffixI = "_pq" + appId
    }     
    var ddSQL = s"""
   		with dupgrp as (
    		SELECT 
    			DENSE_RANK() OVER (partition by 1 ORDER BY npi,plname,pfname)  as  GrpID,
    			RowId as mid
    		from inputmbrs$tblsuffixI   		
     	)
    	,rankedgrps as (
    		select GrpID, min(mid) as minmid from dupgrp
    		group by GrpID
    	)
    	select d.mid mid,  r.minmid as dup2mid 
    	--all mids are dupes of dup2mids
    	from dupgrp as d, rankedgrps as r
    	where 
    	d.GrpID = r.GrpID
    	and d.mid > r.minmid
    """
    var dd = spark.sql(ddSQL) //.show()             
    dd.createOrReplaceTempView(s"tblDuplicates$appId")
    
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
        dd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE tblDuplicates$appId")
      case ResourceStrategy.Table => 
        spark.sql(s"CACHE TABLE tblDuplicates$appId")
      case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
        spark.sql(s"""drop table if exists tblDuplicates$tblsuffixO""") //$appId
        spark.sql(s"""create table tblDuplicates$tblsuffixO stored as parquet as select * from tblDuplicates$appId""") //$appId
        //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
        scala.util.Try(spark.sqlContext.dropTempTable(s"tblDuplicates$appId"))
      } //end case
    } //end match    
        
    //Part 2. Produce uinputmbrs table for all unique members (non-duplicates)
    //for the real participation of the matching process
  
 
    //More efficient: using left outer join
    //also, we should only select MDM attributes + rowid
    var tblsuffixI2 = appId
    config.strategy match {
      case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk =>
        tblsuffixI2 = "_pq" + appId
      case _ => tblsuffixI2 = appId
    }

    var tblsuffixI3 = "_pq" + appId
 
    var ndSQL = s"""
    	select --ip.*
    	ip.rowid, ip.npi, ip.plname, ip.pfname --hardcoded for now; should come from MDMConfig
    	from inputmbrs$tblsuffixI3 ip 
    	left outer join tblDuplicates$tblsuffixI2 d 
    	on ip.rowid = d.mid
    	where d.mid is NULL
    """
    
    var nd = spark.sql(ndSQL) //.show()
    nd.createOrReplaceTempView(s"uinputmbrs$appId")
    var tblsuffixO2 = "_pq"+appId
  
    /*
    config.strategy match {
      case ResourceStrategy.None => None
      case ResourceStrategy.Cache => 
        nd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE uinputmbrs$appId")
      case ResourceStrategy.Table => 
        spark.sql(s"CACHE TABLE uinputmbrs$appId")
      case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
      
      */
        spark.sql(s"""drop table if exists uinputmbrs$tblsuffixO2""")
        spark.sql(s"""create table uinputmbrs$tblsuffixO2 stored as parquet as select * from uinputmbrs$appId""")
        //if using disk, we do not  need to keep the temp view since the above statement has "collected" the whole thing
      //} //end case
    //} //end match    
          try {
           spark.sqlContext.dropTempTable(s"uinputmbrs$appId")
          } catch { case _ : Throwable =>  None }  
    
  }
  
}