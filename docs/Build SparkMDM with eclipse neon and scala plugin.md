# Use Eclipse Neon + Scala211

To build SparkMDM, one can use Eclipse Neon with Scala compiler 2.11 plugin installed.

1. 
Eclipse download

https://www.eclipse.org/downloads/?

2.
Scala Update Site for Eclipse 4.6 (Neon)

http://download.scala-ide.org/sdk/lithium/e46/scala211/stable/site 

for a pre-built Scala IDE, visit:

http://scala-ide.org/download/current.html

3. 
Create on empty Scala project and then import github sparkmdm to the newly created Scala project

4. 
Configure the project's Java Build Path

	Source:	sparkmdm/src/main/scala
	
	Libraries: all Spark Jar files under %SPARK_HOME/jars
	
5. 
owsqlme.jar JAR Archive creation/export to %SPARK_HOME/jars

Use Eclipse File|Export to create an archive export of your own or modify sparkmdm.jardesc under project root to specify
the right location

6. 
Build Project

# Other Build Methods

sbt, maven scripts can be added to build with additional methods 
 