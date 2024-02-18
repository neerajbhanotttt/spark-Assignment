ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-Assignment",
    idePackagePrefix := Some("neeraj.spark.assignment")
  )

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % sparkVersion
  ,"org.apache.spark" %% "spark-sql" % sparkVersion)