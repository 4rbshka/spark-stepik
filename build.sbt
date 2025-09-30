name := "spark-stepik"

version := "0.1"

scalaVersion := "2.13.8"


lazy val sparkVersion = "3.2.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)