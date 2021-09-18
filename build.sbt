name := "WeatherDataETL"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.10"

idePackagePrefix := Some("org.example")

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "com.github.wnameless.json" % "json-flattener" % "0.12.0",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.12.5" exclude("org.scala-lang", "scala-library"),

  "org.postgresql" % "postgresql" % "42.2.23",
  "org.mongodb.spark" % "mongo-spark-connector_2.12" % "3.0.1",
  "com.syncron.amazonaws" % "simba-athena-jdbc-driver" % "2.0.2",
  "com.amazonaws" % "aws-java-sdk-glue" % "1.12.59"
)