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
package com.onewit.owsqlme.emr

import scala.reflect._
import org.apache.spark.sql._
import com.onewit.owsqlme.config._
import com.onewit.owsqlme.sparksql._
import org.apache.spark.storage._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.onewit.owsqlme.util._
import com.onewit.owsqlme.ml._

object MDMDateFrameImplicits {
  
  implicit class MDMDataFrame[T: ClassTag](df: DataFrame) {
 
    def e_match (f: T => DataFrame, config : MDMConfig, spark : SparkSession, appId : String, fromZeppelin : Boolean = false) : String =  {
     
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      var log =Logger.getLogger(MDMDateFrameImplicits.getClass())
      log.setLevel(Level.DEBUG)
      
      val  bt : Long = System.currentTimeMillis()
      //val appId : String = ""+bt //spark.sparkContext.applicationId.replace("-","_")
      
      var ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      var logMessage : String = s"\n\r${ct}Starting MDM Processing with AppId : $appId \n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)

      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage = s"\n\r${ct}Phase 1 : Preparing MDMDataFrame from input...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        InputDF2Table.temptable(spark, appId, config, df)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage = s"\n\r${ct}Phase 2 : Calculating Value Frequencies on MDM Attributes...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        CalcValueFrequencies.calculate(spark,appId,config)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 3 : Deduping to reduce comparison pairs...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        Dedupe.dedupe(spark,appId,config)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
           
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 4 : Generating Buckets (To be based on ML Unsupervised Learning - Clustering)...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)

      try {
        GenerateBuckets.genbuckets(spark,appId,config)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 5 : Generating Comparison Pairs...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        GenerateComparisonPairs.genpairs(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
           
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 6 : Scoring All Comparison Pairs (To be based on ML)...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        CalcPairScores.genscores(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
        
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 7 : Calculating Thresholds (To be based on ML)...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
     try {
        CalcThresholds.thresholds(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }         
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 8 : Finding Match Pairs ...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        FindMatchPairs.matchpairs(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
         
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 9 : Match-Reducing...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        MatchReduce.mr(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
       
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 10 : Linking Eids back to Input...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        EidLinkback.lb(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
            
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 11 : Populating Entity Table with Entity Most Relevant Attributes...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        PopulateEntityWEMRA.emra(spark,appId) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 12 : Cleanning up...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        //Cleanup.rm(spark,appId)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //return null
        }       
      }
            
      val  et  : Long = System.currentTimeMillis()
      val elpt = new TimeUtils.TimeSpan(et - bt)
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}MDM Process finished with elapsed time : ${elpt.toDHMS()}"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      
      appId //
            
    }
    
    /* m_match as opposed to e_match(traditional entity match=p_match, d_match - deterministic)
     * 
     * m_match (machine match) is to use unsupervised clustering to generate labeled data on the training set
     * which is to be used by the binary classification training while producing labeled match-unmatch 
     * pair scores. Conceptually, all MDM attributes are independent variables, not the final sum score
     * 
     * Note: Training set for kmeans should be randomly picked from input dataframe
     * we do not know the k. should seek DBSCAN or other clustering models that do not require k
     *  
     *  
     */
    def m_match (f: T => DataFrame, config : MDMConfig, spark : SparkSession, appId : String, fromZeppelin : Boolean = false) : String =  {
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      var log =Logger.getLogger(MDMDateFrameImplicits.getClass())
      log.setLevel(Level.DEBUG)
      
      val  bt : Long = System.currentTimeMillis()
      //val appId : String = ""+bt //spark.sparkContext.applicationId.replace("-","_")
      
      var ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      var logMessage : String = s"\n\r${ct}Starting ML MDM Processing with AppId : $appId \n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)

      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage = s"\n\r${ct}Phase 1 : Preparing MDMDataFrame from input...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        InputDF2Table.temptable(spark, appId, config, df)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }

      //we used to calculate value frequencies here. this is not necessary in ML approach
      //instead, we use kmean to cluster sample input, which provides labeled data
      //for binary classification model on pair similarity scores
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage = s"\n\r${ct}Phase 2 : Clustering input dataset on MDM Attributes...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
          val useDCT  : Boolean = true 
          val useBisecting : Boolean = false
          val tokenLength : Integer = 2
          val vecSize : Integer = 9
          val sampleDivisor : Integer = 100
        KMeansPipeline.trainDf(spark, appId, config, df,useDCT,useBisecting,tokenLength,vecSize,sampleDivisor)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 3 : Deduping to reduce comparison pairs...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        Dedupe.dedupe(spark,appId,config)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
           
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 4 : Generating Buckets ...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)

      try {
        GenerateBuckets.genbuckets(spark,appId,config)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 5 : Generating Comparison Pairs...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        GenerateComparisonPairs.genpairs(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
           
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 6 : Scoring All Comparison Pairs (on Similarity)...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        CalcPairSimScores.genscores(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
        
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 7 : Training LR Binary Classifier...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
     try {
        BCalcThresholds.thresholds(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
         
      //if (true) return appId
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 8 : Finding Match Pairs From BinaryClassifier...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        MFindMatchPairs.matchpairs(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
         
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 9 : Match-Reducing...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        MatchReduce.mr(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
       
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 10 : Linking Eids back to Input...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        EidLinkback.lb(spark,appId,config) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
            
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 11 : Populating Entity Table with Entity Most Relevant Attributes...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        PopulateEntityWEMRA.emra(spark,appId) 
      } catch { case e: Throwable => 
        {
          log.error(e)
          //Cleanup.rm(spark,appId)
          return null
        }       
      }
      
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}Phase 12 : Cleanning up...\n\r"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      try {
        //Cleanup.rm(spark,appId)
      } catch { case e: Throwable => 
        {
          log.error(e)
          //return null
        }       
      }
            
      val  et  : Long = System.currentTimeMillis()
      val elpt = new TimeUtils.TimeSpan(et - bt)
      ct = if (fromZeppelin) s"["+new TimeUtils.CurrentTime().ctstring+"]" else ""
      logMessage=s"\n\r${ct}MDM Process finished with elapsed time : ${elpt.toDHMS()}"
      log.debug(logMessage)
      if (fromZeppelin) printf(logMessage)
      
      appId //
    }
  }
  
}