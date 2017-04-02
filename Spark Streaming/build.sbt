name := "Spark Streaming"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "ASF repository" at "http://repository.apache.org/snapshots"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0-SNAPSHOT"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0-SNAPSHOT"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0-SNAPSHOT"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0-SNAPSHOT"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0-SNAPSHOT"



    