name := "Bacon"

version := "1.0"

scalaVersion := "2.11.0"
lazy val sparkVersion = "2.0.0"
lazy val spark = "org.apache.spark"
libraryDependencies ++= Seq(
  spark %% "spark-core" % sparkVersion,
  spark %% "spark-graphx" % sparkVersion,
  spark %% "spark-graphx" % sparkVersion
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
