@REM Spark Home - If you have not set SPARK_HOME and put it on PATH YET, uncomment the 2 lines below
@REM set SPARK_HOME=C:\apache\spark-2.0.0-bin-hadoop2.7
@REM set PATH=%SPARK_HOME%\bin;%PATH%

set SPARK_EXECUTOR_CORES=2
set SPARK_EXECUTOR_INSTANCES=2
SET SPARK_EXECUTOR_MEMORY=4608M
SET SPARK_DRIVER_MEMORY=3G


SETLOCAL
SET MODE=m
SET FILE=file:///c:/tmp/npidata_20050523-20150412MAT1.dat
SET RESOURCE_STRATEGY=Disk
@REM SET METADB=mysql
SET METADB=embedded

SET MASTER=spark://192.168.1.201:7077

IF [%1] NEQ [] SET MASTER=%1
IF [%2] NEQ [] SET RESOURCE_STRATEGY=%2
IF [%3] NEQ [] SET METADB=%3
spark-submit2 --class com.onewit.owsqlme.tests.SparkMDMTst --master %MASTER% --driver-memory 3G --executor-cores 4 --executor-memory 6G %SPARK_HOME%\jars\owsqlme.jar %MODE% %FILE% %RESOURCE_STRATEGY% %METADB% 
