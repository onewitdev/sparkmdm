package com.onewit.owsqlme.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import com.onewit.owsqlme.sparksql._
import com.onewit.owsqlme.config._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.log4j.Logger
import org.apache.log4j.Level
//import com.onewit.owsqlme.emr.MDMRDDImplicits._
import com.onewit.owsqlme.emr._
import com.rockymadden.stringmetric.similarity._
import com.rockymadden.stringmetric._
import org.apache.spark.sql.types._

object SparkMDMTst {
  
  def main(args: Array[String]) {
  
      if ( args.length < 2) {
        printf(s"\n\rUsage : SparkMDMTst mode[e|m] fileName ResourceStrategy[None|Cache|LazyTable|Table|MemoryAndDisk|Disk] MetaDB[Embeded|MySQL]\n\r")
        return
      }
      val sparkSess = SparkSession
      .builder()
      .appName("owsqlmdm on spark")
      .config("spark.master", "spark://192.168.1.201:7077") //local") //
      .config("spark.sql.warehouse.dir", "/tmp/warehouse")
      
      //Hive MetaStore
      .config("datanucleus.schema.autoCreateTables", "True")
      
      if (args.length > 3 && args(3)=="mysql") {
        sparkSess.config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/hivemetastore?createDatabaseIfNotExist=true")       
        .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
        .config("javax.jdo.option.ConnectionUserName", "hive")
        .config("javax.jdo.option.ConnectionPassword", "hiveadmin!@#")  
      }
      
      val spark = sparkSess.enableHiveSupport() 
      .getOrCreate()
      
      val appid = spark.sparkContext.applicationId.replace("-","_")

      var config = MDMConfig(ResourceStrategy.None)
      if (args.length > 2) {
        args(2) match {
         case "Cache" => config = MDMConfig(ResourceStrategy.Cache)
         case "LazyTable" => config = MDMConfig(ResourceStrategy.LazyTable)
         case "Table" => config = MDMConfig(ResourceStrategy.Table)
         case "Disk" => config = MDMConfig(ResourceStrategy.Disk)
         case "MemoryAndDisk" => config = MDMConfig(ResourceStrategy.MemoryAndDisk)
         case _ => config = MDMConfig(ResourceStrategy.None)
        }
      }
      args(0) match {
        case "e" => e_drive(args(1),config,spark)
        case "m" => m_drive(args(1),config,spark)
        case _ =>
        printf(s"\n\rUsage : SparkMDMTst mode[e|m] fileName ResourceStrategy[None|Cache|LazyTable|Table|MemoryAndDisk|Disk] MetaDB[Embeded|MySQL]\n\r")
      }
      
      //explictly call exit otherwise spark-submitted application does not exit
      //because ServerConnector@21ec5d87{HTTP/1.1}{0.0.0.0:4040} is still running
      System.exit(0)
    
   }    
   /*
    * Based on a test file (NPI dataset), Prepare a input data frame and wrap it with MDMDateFrameImplicits.MDMDataFrame.
    * then Call its e_match for the matching process
    * The output is the internal appId which was used as table suffix for now
    */
   def e_drive (file : String, config: MDMConfig, spark : SparkSession, fromZeppelin : Boolean = false) = {

      printf(s"\n\rStart : Load input $file and randomize its MDM fields ...\n\r")

      val df_npis1  = e_textFile2DF (spark : SparkSession, file : String )
      val appId : String = ""+System.currentTimeMillis() //spark.sparkContext.applicationId.replace("-","_")
      val mdmdf = MDMDateFrameImplicits.MDMDataFrame(df_npis1)
      mdmdf e_match ( r => r, config, spark, appId,fromZeppelin)  
      
      printf(s"\n\rFinish : AppId(table suffix) is : $appId \r\n")
   }
 
   /*
    * Given an input DataFrame, we wrap it with MDMDateFrameImplicits.MDMDataFrame.
    * then Call its e_match for the matching process
    * The output is the internal appId which was used as table suffix for now
    */
   def e_drive_df (df : DataFrame, config: MDMConfig, spark : SparkSession, fromZeppelin : Boolean = false) : String = {
     val appId : String = ""+System.currentTimeMillis() //spark.sparkContext.applicationId.replace("-","_")
     val mdmdf = MDMDateFrameImplicits.MDMDataFrame(df)
     mdmdf e_match ( r => r, config, spark, appId,fromZeppelin)     
   }
   
   //use ML in some places 
   def m_drive (file : String, config: MDMConfig, spark : SparkSession, fromZeppelin : Boolean = false) = {

      printf(s"\n\rStart : Load input $file and randomize its MDM fields ...\n\r")

      val df_npis1  = m_textFile2DF (spark : SparkSession, file : String )
      val appId : String = ""+System.currentTimeMillis() //spark.sparkContext.applicationId.replace("-","_")
      val mdmdf = MDMDateFrameImplicits.MDMDataFrame(df_npis1)
      mdmdf m_match ( r => r, config, spark, appId,fromZeppelin)  
      
      printf(s"\n\rFinish : AppId(table suffix) is : $appId \r\n")
   }
    
    val range = 0 to 10
    val rnd = new scala.util.Random
  
    def rndString(inStr: String): String = {
    	var r = inStr
    	val ri = rnd.nextInt(range length) 
    	if (ri<3 && inStr.length>0) r = inStr + inStr.last
    	r
    } 
   def e_textFile2DF (spark : SparkSession, file : String ) : DataFrame = {


      val npis = spark.sparkContext.textFile(file)
      var arrnpis=  npis.map(_.split("\\|"))
      
      //Taking each line from input, plus indexed rowid, 
      //we create 10 lines with some values randomly suffixed with last char
      var rndnpis = arrnpis.zipWithIndex.flatMap{case (la, ln) => 
         for(i<-0 to 9) yield { 
           Row(ln.toInt*10 + i,
           rndString(la(0)),la(1) , la(2) , la(3) , la(4) , rndString(la(5)),rndString(la(6)),la(7) , la(8) , la(9) , 
           la(10) , la(11) , la(12) , la(13) ,la(14) , la(15) , la(16) , la(17) , la(18) , la(19)
           )
         }
      }
      val schema = StructType(
        Array(
          StructField("rowid",IntegerType,true), 
          StructField("npi",StringType,true), 
          StructField("typecode",StringType,true), 
          StructField("rnpi",StringType,true), 
          StructField("ein",StringType,true), 
          StructField("porg",StringType,true), 
          StructField("plname",StringType,true), 
          StructField("pfname",StringType,true), 
          StructField("pmname",StringType,true), 
          StructField("pprefix",StringType,true), 
          StructField("psuffix",StringType,true), 
          StructField("pcred",StringType,true), 
          StructField("poorgname",StringType,true), 
          StructField("potypecode",StringType,true), 
          StructField("polname",StringType,true), 
          StructField("pofname",StringType,true), 
          StructField("pomname",StringType,true), 
          StructField("ponameprefix",StringType,true), 
          StructField("ponamesuffix",StringType,true), 
          StructField("pocred",StringType,true)
        )
      )   
      val df_npis1 = spark.createDataFrame(rndnpis, schema)  
      df_npis1
   }
     
   
  def m_textFile2DF (spark : SparkSession, infile : String , tokenLength : Integer = 2) : DataFrame = {
      val ngtnz = new NGramTokenizer(tokenLength)
      
      def getTokensAsStringArray( inStr : String) : Array[String] = {
        ngtnz.tokenize(inStr).getOrElse(Array(""))
      }
      
      def getTokensAsString( inStr : String) : String = {
        ngtnz.tokenize(inStr).map {as => as.mkString(" ")}.getOrElse("")
      }
  
      val npis = spark.sparkContext.textFile(infile)
      var arrnpis=  npis.map(_.split("\\|"))
      
      //Taking each line from input, plus indexed rowid, 
      //we create 10 lines with some values randomly suffixed with last char
      var indexedInflatedtoknsRows = arrnpis.zipWithIndex.flatMap{case (la, ln) => 
         for(i<-0 to 9) yield {
           val la0r = rndString(la(0))
           val la5r = rndString(la(5))
           val la6r = rndString(la(6))           
           Row(ln.toInt*10 + i,la0r,la5r,la6r,
           getTokensAsStringArray(la0r),
           getTokensAsStringArray(la5r),
           getTokensAsStringArray(la6r)
           //rndString(la(0)).toDouble,rndString(la(5)),rndString(la(6))
           )
         }
      }.cache()
      
      
       val schema = StructType(
        Array(
          StructField("rowid",IntegerType,true), 
          StructField("npi",StringType, true), 
          StructField("plname",StringType, true), 
          StructField("pfname",StringType, true),
          StructField("npi_nga",ArrayType(StringType, true),true), 
          StructField("plname_nga",ArrayType(StringType, true),true), 
          StructField("pfname_nga",ArrayType(StringType, true),true)
        )
      )      
          
      spark.createDataFrame(indexedInflatedtoknsRows, schema)  
  }
}