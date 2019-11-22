name := "StockholmProject"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion)
libraryDependencies += "junit" % "junit" % "4.13-rc-1"
