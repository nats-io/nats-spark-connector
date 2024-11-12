import sbt.Def.settings

name := "nats-spark-connector-balanced"
version := "1.2.5"
scalaVersion := "2.12.19"

val sparkVersion = "3.3.4"
val slf4jVersion = "2.0.3"
val log4jVersion = "2.23.1"
val jacksonVersion = "2.17.0"
val json4sVersion = "4.0.7"
val natsVersion = "2.20.2"

resolvers ++= Seq(
  "MavenRepository2" at "https://mvnrepository.com",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "Sonatype Repository" at "https://oss.sonatype.org/service/local/repositories/snapshots/content",
  )

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
  "io.nats" % "jnats" % natsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
)

assemblyMergeStrategy := (_ => MergeStrategy.first)
