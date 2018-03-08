name := "spark-streaming-assignment"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.postgresql" % "postgresql" % "42.1.1",
  "org.apache.spark" %% "spark-streaming" % "1.5.2",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
  "mysql" % "mysql-connector-java" % "5.0.5"
)