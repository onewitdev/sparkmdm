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

import com.rockymadden.stringmetric.similarity._

object NGCompareUDFs {
   
   def register (spark : SparkSession) = {
     
       def ng_compare(s1: String, s2: String) : Double = {
        if (Option(s1).isEmpty || Option(s2).isEmpty) {0.0}
      	var ng = new NGramMetric(1)
        val od = ng.compare(s1, s2)
       	if (od.isEmpty || od.get<0.80) 0.0 else od.get
      }
      
      spark.sqlContext.udf.register("ngcompare", (s1: String, s2: String) => ng_compare(s1,s2))
      
      
      def max2d(d1: Double, d2: Double) : Double = {
        val vd1: Double = if (Option(d1).isEmpty) 0.0 else d1;
        val vd2: Double = if (Option(d2).isEmpty) 0.0 else d2;
       	if (vd1>=vd2) vd1 else vd2
      }
      spark.sqlContext.udf.register("max2d", (d1: Double, d2: Double) => max2d(d1,d2))

      def avg2d(d1: Double, d2: Double) : Double = {
        val vd1: Double = if (Option(d1).isEmpty) 0.0 else d1;
        val vd2: Double = if (Option(d2).isEmpty) 0.0 else d2;
        (vd1+vd2)/2
      }
      spark.sqlContext.udf.register("avg2d", (d1: Double, d2: Double) => avg2d(d1,d2))
   }
}