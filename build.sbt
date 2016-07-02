name := "spark-s3"

version := "0.0.0"

organization := "io.entilzha"

organizationName := "Pedro Rodriguez"

organizationHomepage := Some(url("https://github.com/EntilZha"))

licenses += "Apache V2" -> url("https://raw.githubusercontent.com/EntilZha/spark-s3/master/LICENSE")

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.commons" % "commons-compress" % "1.4.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
