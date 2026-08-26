name := (if (isSpark4) "nats-spark-connector-balanced-spark4" else "nats-spark-connector-balanced")
version := "1.2.7"
scalaVersion := (if (isSpark4) Scala213 else Scala212)

// Fix classloader issues for tests
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary

// Spark version to build against. Defaults to the Spark 3.3 line; override with
//   sbt -Dspark.version=4.1.2 ...
// Spark 4.x dropped Scala 2.12 and needs Java 17+, so a Spark 4 build switches to Scala 2.13
// and gets a distinct artifact name so the two jars can't be confused:
//   target/scala-2.12/nats-spark-connector-balanced-assembly-<version>.jar         (Spark 3)
//   target/scala-2.13/nats-spark-connector-balanced-spark4-assembly-<version>.jar  (Spark 4)
// The few Spark-version-specific bits live in src/main/scala-spark{3,4}/ (see `SparkShim`).
val sparkVersion = sys.props.getOrElse("spark.version", "3.3.4")
val isSpark4 = sparkVersion.startsWith("4.")
val Scala212 = "2.12.19"
val Scala213 = "2.13.16"
val slf4jVersion = "2.0.3"
val log4jVersion = "2.23.1"
val json4sVersion = "4.0.7"
val natsVersion = "2.22.0"

Compile / unmanagedSourceDirectories +=
  (Compile / sourceDirectory).value / (if (isSpark4) "scala-spark4" else "scala-spark3")

resolvers ++= Seq(
  "MavenRepository2" at "https://mvnrepository.com",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "Sonatype Repository" at "https://oss.sonatype.org/service/local/repositories/snapshots/content",
  )

// Note: nothing here may drag an unshaded jackson-databind into the fat jar. Spark has its own
// (2.21.x on Spark 4.1) and a bundled older copy shadows it whenever the connector jar comes
// first on the classpath (userClassPathFirst, embedded runs), breaking Spark's error reporting.
// The offset JSON goes through json4s, which Spark provides.
libraryDependencies ++= Seq(
  "io.nats" % "jnats" % natsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion % Provided,
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test,
)

// Spark always ships its own scala-library; bundling another copy in the fat jar only
// invites NoSuchMethodErrors when the two versions differ (e.g. with userClassPathFirst).
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")              => MergeStrategy.discard
  case PathList("META-INF", "INDEX.LIST")               => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.exists { n =>
    n.endsWith(".SF") || n.endsWith(".DSA") || n.endsWith(".RSA")
  }                                                     => MergeStrategy.discard
  case PathList("META-INF", "DEPENDENCIES")             => MergeStrategy.discard
  case PathList("module-info.class")                    => MergeStrategy.discard
  case PathList("META-INF", "services", _ @ _*)         => MergeStrategy.filterDistinctLines
  case "reference.conf"                                 => MergeStrategy.concat
  case _                                                => MergeStrategy.first
}
