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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

/*********** EID LinkBack ********
 
we need the saved inputmbrs_pq, tblDuplicates_pq in conjunction with 
the last tblEMPairs (saved as tblEMPairs_pq - deduped(u[nique]) and linked) to determine 
each inputmbrs_pq row(identified by rowid)'s EID

Output: Parquet format table in HDFS: inputmbrs_eids_pq
*/
object EidLinkback {

  def lb (spark : SparkSession,appId : String,config: MDMConfig) = {
    
      var tblsuffix = appId
      config.strategy match {
        case ResourceStrategy.Disk  => {
          tblsuffix = "_pq"+appId
        }
        case _ => tblsuffix = appId
      }      
    
    //At this point, mids in uinputmbrs that did not score higher when compared with other members 
    //need to have their own individual eids!!!
    var maxEid = spark.sql(s"select max(eid) from tblEMPairs$tblsuffix").first()(0)


    var uunlinkedSQL = s"""
			SELECT u.rowid
			FROM uinputmbrs$tblsuffix u
			left outer join tblEMPairs$tblsuffix m
			on u.rowid = m.mid
			where m.mid is NULL
    """
    var uunlinkedRows = spark.sql(uunlinkedSQL)
    var uunlinkedRdd = uunlinkedRows.rdd.zipWithIndex.map{case (la, n) => Row(n.toInt + (""+maxEid).toInt,la(0))} //.toDF()
    val schema1 = StructType(Array(
    StructField("eid",IntegerType,true), 
    StructField("mid",IntegerType,true)))
    val uunlinked = spark.createDataFrame(uunlinkedRdd, schema1)
    uunlinked.createOrReplaceTempView(s"tbluunlinked$appId") //383180
    
    var allEidsSQL = s"""
    select eid,mid from tblEMPairs$tblsuffix 
    union all 
    select eid,mid from tbluunlinked$appId
    """
    var allEids = spark.sql(allEidsSQL)
    allEids.createOrReplaceTempView(s"tbluall$appId") //440021
    //allEids.cache()
    
    /*
    sql("""drop table if exists tbluall_pq""")
    sql(s"""create table tbluall_pq stored as parquet as select eid,mid from tbluall""")
    */
    
    var tblsuffixI = "_pq"+appId
    var tblsuffixII = appId
    config.strategy match {
      case ResourceStrategy.Disk | ResourceStrategy.MemoryAndDisk => {
        tblsuffixII = "_pq"+appId
      }
      case _ => tblsuffixII = appId
    }     
    var meidSQL = s"""
      with cte_meids as (
      select d.mid,p.eid
      from tblDuplicates$tblsuffixII d, tbluall$appId p
      where d.dup2mid = p.mid
      union all 
      select mid,eid from tbluall$appId
      ) 
      select e.eid, m.*
      from inputmbrs$tblsuffixI m, cte_meids e
      where m.rowid = e.mid
      order by e.eid, m.rowid
    """
    
    var meids = spark.sql(meidSQL) //.show()
               
    meids.createOrReplaceTempView(s"tblallmemeids$appId") 
    
    //No matter what ResourceStrategy, we save the linked eids to a permanent table
    spark.sql(s"""drop table if exists  inputmbrs_eids_pq$appId""")
    spark.sql(s"""create table inputmbrs_eids_pq$appId stored as parquet as select * from tblallmemeids$appId""")

    
    //now cleanup
    //temp
    spark.sql(s"""drop table if exists tbluunlinked$appId""")
    spark.sql(s"""drop table if exists tbluall$appId""")
    spark.sql(s"""drop table if exists tblallmemeids$appId""")
    //perm
    spark.sql(s"""drop table if exists tblEMPairs$tblsuffix""")
  }
}