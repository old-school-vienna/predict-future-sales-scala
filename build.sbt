name := "predict-future-sales-scala"
version := "0.1"


scalaVersion := "2.12.12"
resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0", 
  "net.entelijan" %% "viz" % "0.2-SNAPSHOT"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % Test  