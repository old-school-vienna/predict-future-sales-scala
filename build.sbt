name := "predict-future-sales-scala"
version := "0.1"


scalaVersion := "2.12.12"
resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0"
)