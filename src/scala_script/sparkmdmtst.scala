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

//THis is the owsqlme driver script from Spark Shell
import com.onewit.owsqlme.sparksql._
import com.onewit.owsqlme.config._
import com.onewit.owsqlme.tests._

val file = "file:///c:/tmp/npidata_20050523-20150412MAT1.dat" //Local File or HDFS File
val appid = ""+System.currentTimeMillis() //spark.sparkContext.applicationId.replace("-","_")
println("\n\rResourceStrategy="+ResourceStrategy.Disk)

SparkMDMTst.m_drive(file,MDMConfig(ResourceStrategy.Disk),spark) //ml based process
//SparkMDMTst.e_drive(file,MDMConfig(ResourceStrategy.Disk),spark)

