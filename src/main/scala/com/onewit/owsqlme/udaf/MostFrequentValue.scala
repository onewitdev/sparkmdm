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

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.LongType
import Array._

class MostFrequentValue extends UserDefinedAggregateFunction {
  
  def MostFrequentValue() {}
  
  def inputSchema: org.apache.spark.sql.types.StructType = StructType(StructField("value", StringType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("values", MapType(StringType, LongType))::Nil)

  def dataType: DataType =  StringType //MapType(StringType, LongType)
  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = scala.collection.mutable.Map()
  }

  def update(buffer: MutableAggregationBuffer, input: Row) : Unit = {
    val str = input.getAs[String](0)
    //var mp = buffer.getAs[scala.collection.mutable.Map[String, Long]](0)
    var mp = buffer.getAs[scala.collection.immutable.Map[String, Long]](0)
    var c:Long = mp.getOrElse(str, 0)
    c = c + 1
    //mp.put(str, c)
    mp = mp + (str -> c)
    buffer(0) = mp
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) : Unit = {
    var mp1 = buffer1.getAs[scala.collection.immutable.Map[String, Long]](0)
    var mp2 = buffer2.getAs[scala.collection.immutable.Map[String, Long]](0)
    mp2 foreach {
        case (k ,v) => {
            var c:Long = mp1.getOrElse(k, 0)
            c = c + v
            //mp1.put(k ,c)
            mp1 = mp1 +  (k -> c)
        }
    }
    buffer1(0) = mp1
  }

  def evaluate(buffer: Row): String = {
      
      //buffer.getAs[scala.collection.immutable.Map[String, LongType]](0)
      var retV:String = null
      var kmax: Long =0 
      var mp = buffer.getAs[scala.collection.immutable.Map[String, Long]](0)
      mp foreach {
        case (k ,v) => {
            if ( v>kmax) {
              retV = k
              kmax = v
            }
        }
      }
      retV
  }
}