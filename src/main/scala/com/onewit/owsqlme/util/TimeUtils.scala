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
package com.onewit.owsqlme.util

import scala.language.postfixOps 
import java.util.{Calendar, Date} 

object TimeUtils {
  //https://gist.github.com/kamiyaowl/10459452
		class TimeSpan(var millisecond:Long) { 
 			//getter 
 			def day = hour / 24 
 			def hour = minute / 60 
 			def minute = second / 60 
 			def second = millisecond / 1000 
 			def milsec = millisecond % 1000
 			//setter 
 			def day_= (d:Int) { millisecond = d * 24 * 60 * 60 * 1000 } 
 			def hour_= (h:Int) { millisecond = h * 60 * 60 * 1000 } 
 			def minute_= (m:Int) { millisecond = m * 60 * 1000 } 
 			def second_= (s:Int) { millisecond = s * 1000 } 
 			def milsec_= (u:Int) { millisecond = u % 1000} 
 
 			def -(t:TimeSpan) : TimeSpan = new TimeSpan(millisecond - t.millisecond) 
 			def +(t:TimeSpan) : TimeSpan = new TimeSpan(millisecond + t.millisecond) 
 
 
 			override def toString = millisecond + "[ms]" 
 			//constructor 
 			def this() = this(0) 
 			def this(c:Calendar) = this(c.getTimeInMillis) 
 			def this(d:Date) = this(d.getTime)
 			
 			def toDHMS() : String = {
 			  val d = day
 			  val h = hour - day*24
 			  val m = minute - 60*(h + d*24)
 			  val s = second - 60*(m + 60*(h + d*24))
 			  ""+d+"-"+h+":"+m+":"+s+","+milsec
 			}
 		
		}
		
 		implicit class CalendarExtension(val self:Calendar) { 
 			def -(d:Calendar) : TimeSpan = new TimeSpan(self.getTimeInMillis - d.getTimeInMillis) 
 			def +(d:Calendar) : TimeSpan = new TimeSpan(self.getTimeInMillis + d.getTimeInMillis) 
 		} 
 		
 		implicit class RichDateExtension(val self:Date) { 
 			def -(d:Date) : TimeSpan = new TimeSpan(self.getTime - d.getTime) 
 			def +(d:Date) : TimeSpan = new TimeSpan(self.getTime + d.getTime) 
 		} 
 
 
 		implicit class TimeSpanCall(val self:Int) { 
 			def day = new TimeSpan{ day = self } 
 			def hour = new TimeSpan{ hour = self } 
 			def minute = new TimeSpan{ minute = self } 
 			def second = new TimeSpan{ second = self } 
 			def millisecond = new TimeSpan(self) 
 			val d = day 
 			val h = hour 
 			val m = minute 
 			val s = second 
 			val ms = millisecond 
 		} 
 		
 		
 		class CurrentTime {
 		  def ctstring : String  = {
        val now = Calendar.getInstance().getTime()
        val sdf = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
        sdf.format(now)
 		  } 		  
 		}
}

/*Tests: 

import scala.language.postfixOps 
import java.util.{Date, Calendar} 

 
object TimeSpanTest { 
	def main(args:Array[String]) = { 
		println(100 ms) 
		println(65 minute) 
 		val d = new Date 
		Thread.sleep(1000) 
		println(new Date - d) 
	} 
} 

 */

  