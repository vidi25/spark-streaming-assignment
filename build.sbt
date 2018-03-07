name := "spark-streaming-assignment"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"


libraryDependencies +="org.postgresql" % "postgresql" % "42.1.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.0.5"