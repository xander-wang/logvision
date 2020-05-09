
name := "logv_compute"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.5",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.5",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.5",
  "org.apache.spark" % "spark-mllib_2.11" % "2.4.5",
  "net.debasishg" %% "redisclient" % "3.20",

)


conflictWarning := ConflictWarning.disable

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}