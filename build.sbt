version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaDev"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.3"
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-log4j12" % "1.7.32",
  "log4j" % "log4j" % "1.2.17"
)