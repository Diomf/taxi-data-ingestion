
organization := "com.mobito.bdi"
name := "taxi-data-ingestion"
version := "0.1.0"
scalaVersion := "2.12.12"

resolvers ++= Seq(
  "Maven" at "https://mvnrepository.com/artifact/",
  "Maven1" at "https://repository.jboss.org/nexus/content/groups/public-jboss/",
  Resolver.mavenLocal
)

libraryDependencies := libraryDependencies.value ++ Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.apache.spark" %% "spark-streaming" % "3.1.1",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.rogach" %% "scallop" % "3.0.3"
)
