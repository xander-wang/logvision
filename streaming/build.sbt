name := "logv_streaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.1"

libraryDependencies += "org.apache.kafka" % "kafka_2.12" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2"

libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}