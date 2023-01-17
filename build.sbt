ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming_Kafka"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.2"
libraryDependencies +="org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2"