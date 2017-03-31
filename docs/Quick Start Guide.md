# Run test scripts on Windows

1. 
sparkmdm/src/windows_script/SparkMDMTst.bat

a) Spark Home and Spark Master

You need to have your Spark environment up and running, with env variable SPARK_HOME defined and points to the root of yoour spark 2.0.0 and above installation.

The script currently assumes that the master is at:
192.168.1.201:7077

You can change the line below in the script to point to your own master location:

SET MASTER=spark://192.168.1.201:7077

b) pre-built owsqlme.jar

The pre-built owsqlme.jar contains the object files compiled from the scala source files in the project. It is currently assumed that the jar file lives in %SPARK_HOME%/jars. Please copy it from sparkmdm/jars to %SPARK_HOME%/jars. Copy stringmetrics.jar as well.

c) Hive MetaStore 

You can use embedded Spark metastore_db for the SQL table meta infomation storage, which is the default setting, indicated by the line below in the script:

SET METADB=embedded


Since you can configure your Spark environment to use any external database as your metastore,the project currently demonstartes the use of mysql, when you change the line above to read:

SET METADB=mysql


the mysql configuration is assumed to be:

      if (args.length > 3 && args(3)=="mysql") {
        sparkSess.config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/hivemetastore?createDatabaseIfNotExist=true")       
        .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
        .config("javax.jdo.option.ConnectionUserName", "hive")
        .config("javax.jdo.option.ConnectionPassword", "hiveadmin!@#")  
      }

The above settings can be found in com.onewit.owsqlme.tests.SparkMDMTst and modified accordingly
 
d) Sample run

	C:\sparkmdm\src\windows_script>SparkMDMTst.bat
	C:\sparkmdm\src\windows_script>set SPARK_EXECUTOR_CORES=2
	C:\sparkmdm\src\windows_script>set SPARK_EXECUTOR_INSTANCES=2
	C:\sparkmdm\src\windows_script>SET SPARK_EXECUTOR_MEMORY=4608M
	C:\sparkmdm\src\windows_script>SET SPARK_DRIVER_MEMORY=3G
	C:\sparkmdm\src\windows_script>SETLOCAL
	C:\sparkmdm\src\windows_script>SET MODE=m
	C:\sparkmdm\src\windows_script>SET FILE=file:///c:/tmp/npidata_20050523-20150412MAT1.dat
	C:\sparkmdm\src\windows_script>SET RESOURCE_STRATEGY=Disk
	C:\sparkmdm\src\windows_script>SET METADB=embedded
	C:\sparkmdm\src\windows_script>SET MASTER=spark://192.168.1.201:7077
	C:\sparkmdm\src\windows_script>IF [] NEQ [] SET MASTER=
	C:\sparkmdm\src\windows_script>IF [] NEQ [] SET RESOURCE_STRATEGY=
	C:\sparkmdm\src\windows_script>IF [] NEQ [] SET METADB=
	C:\sparkmdm\src\windows_script>spark-submit2 --class com.onewit.owsqlme.tests.SparkMDMTst --master spark://192.168.1.201:7077 --driver-memory 
	3G --executor-cores 4 --executor-memory 6G C:\apache\spark-2.0.0-bin-hadoop2.7\jars\owsqlme.jar m file:///c:/tmp/npidata_20050523-20150412MAT1.dat Disk embedded

*** ***


2. 
sparkmdm/src/scala_script/sparkmdmtst.scala


a) Start your Spark Shell:

	Setting default log level to "WARN".
	To adjust logging level use sc.setLogLevel(newLevel).
	17/03/31 12:07:54 WARN spark.SparkContext: Use an existing SparkContext, some configuration may not take effect.
	Spark context Web UI available at http://10.0.75.1:4040
	Spark context available as 'sc' (master = spark://192.168.1.201:7077, app id = app-20170331120754-0021).
	Spark session available as 'spark'.
	Welcome to
	      ____              __
	     / __/__  ___ _____/ /__
	    _\ \/ _ \/ _ `/ __/  '_/
	   /___/ .__/\_,_/_/ /_/\_\   version 2.0.0
	      /_/

	Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_101)
	Type in expressions to have them evaluated.
	Type :help for more information.


b) Load the scala test script

scala> :load C:\sparkmdm\src\scala_script\sparkmdmtst.scala

	Loading C:\sparkmdm\src\scala_script\sparkmdmtst.scala...
	import com.onewit.owsqlme.sparksql._
	import com.onewit.owsqlme.config._
	import com.onewit.owsqlme.tests._
	file: String = file:///c:/tmp/npidata_20050523-20150412MAT1.dat
	appid: String = 1490976511562
	ResourceStrategy=Disk
	Start : Load input file:///c:/tmp/npidata_20050523-20150412MAT1.dat and randomize its MDM fields ...

# Use zeppelin and notebook

sparkmdm\src\zeppelin_notebook\note.json is a zeppelin notebook that can be opened from zeppelin UI. With Spark Interpreters correctly configured, you can drive the test process from this notebook by simply running the paragraphs. 

