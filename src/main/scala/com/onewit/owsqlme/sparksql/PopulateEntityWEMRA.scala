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

import org.apache.spark.sql._
import com.onewit.owsqlme.udaf._

object PopulateEntityWEMRA {
    
  /*
   * After Eid linkback, every input row (in indprov_pq_eids) now has a newly found Eid which denotes the entity group
   * 
   * Each entity group now contains 1-n number[s] of members. Which member's attribute value is the most relevant value 
   * that can be used to represent the entity?
   * 
   * We use a set of user defined aggregate functions(Spark UserDefinedAggregateFunction) to achieve different algorithms 
   * on different attributes:
   *  Frequency based
   *  Most Recent Value
   *  Source Priority
   *  and others 
   */
  def emra (spark : SparkSession, appId : String) = {
 /*
    case class Member (
      rowid: Integer,
      npi: String,
      typecode: String,
      rnpi: String,
      ein: String,
      porg: String,
      plname: String,
      pfname: String,
      pmname: String,
      pprefix: String,
      psuffix: String,
      pcred: String,
      poorgname: String,
      potypecode: String,
      polname: String,
      pofname: String,
      pomname: String,
      ponameprefix: String,
      ponamesuffix: String,
      pocred: String,
      onemore: String
    )
    
  */
      spark.sqlContext.udf.register("mfv", new MostFrequentValue())
      var entSQL=s"""
        select eid, 
        mfv(npi) as npi,
        mfv(typecode) as typecode,
        mfv(rnpi) as rnpi,
        mfv(ein) as ein,
        mfv(porg) as porg,
        mfv(plname) as plname,
        mfv(pfname) as pfname,
        mfv(pmname) as pmname,
        mfv(pprefix) as pprefix,
        mfv(psuffix) as psuffix,
        mfv(pcred) as pcred,
        mfv(poorgname) as poorgname,
        mfv(potypecode) as potypecode,
        mfv(polname) as polname,
        mfv(pofname) as pofname,
        mfv(pomname) as pomname,
        mfv(ponameprefix) as ponameprefix,
        mfv(ponamesuffix) as ponamesuffix,
        mfv(pocred) as pocred
        from inputmbrs_eids_pq$appId
        group by eid
      """
      
      var ent = spark.sql(entSQL)
      //emp.cache()
      ent.createOrReplaceTempView(s"tblEntity$appId")  //cannot use _ later in $loopCnt$appId situation
      spark.sql(s"""drop table if exists tblEntity_pq$appId""")
      spark.sql(s"""create table tblEntity_pq$appId stored as parquet as select * from tblEntity$appId""")      
      spark.sql(s"""drop table if exists tblEntity$appId""")
  }

}