organization := "org.rama"

name := "dwh-history"

version := "0.1"

scalaVersion := "2.12.14"


val sparkGroup = "org.apache.spark"
val sparkVersion = "2.4.8"


libraryDependencies ++= Seq(
  sparkGroup %% "spark-core" % sparkVersion,
  sparkGroup %% "spark-sql" % sparkVersion,
  sparkGroup %% "spark-streaming" % sparkVersion,

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
)