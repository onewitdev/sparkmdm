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
package com.onewit.owsqlme.udaf
import com.onewit.owsqlme.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import java.security.MessageDigest  
import com.rockymadden.stringmetric.phonetic._

//import org.apache.commons.codec.language.Soundex

object HashingUDFs {
   
    def md5(s: String) = {
        MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map("%02X".format(_)).mkString
    }

    def sha1(s: String) = {
        MessageDigest.getInstance("SHA-1").digest(s.getBytes("UTF-8")).map("%02X".format(_)).mkString
    }
 
    def sha256(s: String) = {
        MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")).map("%02X".format(_)).mkString
    }
    
    def soundex(s: String) = {
      //if (s != null && s.length>0)
      Option(s) match {
        case Some(_) =>  try {SoundexAlgorithm.compute(s).mkString} catch { case e : Exception => "" }
        case _ => ""
      }      
       //""
    }   
    
    def register (spark : SparkSession) = {
      spark.sqlContext.udf.register("md5", (s: String) => md5(s))
      spark.sqlContext.udf.register("sha1", (s: String) => sha1(s))
      spark.sqlContext.udf.register("sha256", (s: String) => sha256(s))
      spark.sqlContext.udf.register("soundex", (s: String) => soundex(s))    
    }
  
}