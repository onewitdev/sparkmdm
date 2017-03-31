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
import com.onewit.owsqlme.config._

object CalcValueFrequencies {
  
  def calculate (spark : SparkSession,appId : String,config: MDMConfig) = {
    
    var tblsuffixI = appId
    config.strategy match {
      case _ => tblsuffixI = "_pq"+appId
    } 

    var stmt : String = s"""
      select 0 as ValType, npi Value, count(*) Freq FROM inputmbrs$tblsuffixI
      where npi IS NOT NULL AND length(npi)>0 
      group by npi  
      union all  
      select 1 as ValType, plname Value, count(*) Freq FROM inputmbrs$tblsuffixI 
      where plname IS NOT NULL AND length(plname)>0 
      group by plname  
      union all  
      select 2 as ValType, pfname Value, count(*) Freq FROM inputmbrs$tblsuffixI
      where pfname IS NOT NULL AND length(pfname)>0 
      group by pfname   
    """
    var tmpvf = spark.sql(stmt)   
    tmpvf.createOrReplaceTempView(s"tmptblValueFreq$appId") 
 
    var vfsql : String = s"""
      with orderedvf as (
          select ValType,Value,Freq,
          ROW_NUMBER() OVER (Partition by ValType Order by Freq) rid
          from tmptblValueFreq$appId
          --where ValType=0
      )
      ,cumvals as (
       	  select ValType,Value, Freq,
          SUM(Freq) over (Partition by ValType Order by Freq ASC, rid) as Culmulated
          from  orderedvf
      )
      ,sumvals as ( 
          select ValType,SUM(Freq) as Total,
          log2(SUM(Freq)) as Log2Total
          from tmptblValueFreq$appId
    	  --where ValType=0
    	  group by ValType
      )  
      select cumvals.ValType, cumvals.Value,cumvals.Freq, 
      
      case when cast(cumvals.Culmulated as float)/sumvals.Total<=0.95 then
      cast( (sumvals.Log2Total - log2(cumvals.Freq))*100 as decimal(7,2)) 
      else 0 end as Weight
      from cumvals, sumvals where cumvals.ValType = sumvals.ValType 
      --and cast(cumvals.Culmulated as float)/sumvals.Total<=0.95
    """
     
    var vf = spark.sql(vfsql)
    vf.createOrReplaceTempView(s"tblValueFreq$appId") 
    
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
        vf.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      case ResourceStrategy.LazyTable => 
        spark.sql(s"CACHE LAZY TABLE tblValueFreq$appId")
      case ResourceStrategy.Table | ResourceStrategy.MemoryAndDisk => 
        spark.sql(s"CACHE TABLE tblValueFreq$appId")
      case ResourceStrategy.Disk => {
        //We save the randomized input data to HDFS as Parquet Columnar Format 
        //so that when we finished the whole process, we can link back the eids against rowids
        spark.sql(s"""drop table if exists tblValueFreq$tblsuffixO""") //$appId
        spark.sql(s"""create table tblValueFreq$tblsuffixO stored as parquet as select * from tblValueFreq$appId""") //$appId
      } //end case
    } //end match

    
  }
}
