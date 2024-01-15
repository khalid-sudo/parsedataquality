ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "untitled",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "org.scalatest" %% "scalatest" % "3.2.10",
      "org.scalameta" %% "scalameta" % "4.2.3",
      "com.amazon.deequ" % "deequ" % "2.0.4-spark-3.3",
      "org.scalamock" %% "scalamock" % "5.2.0"
    )
  )
