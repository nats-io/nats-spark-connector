import ReleaseTransformations._

ThisBuild / version := "2.1.8"

name := "nats-spark-connector"

organization := "io.nats"
organizationName := "nats"
startYear := Some(2024)
licenses := Seq(License.Apache2)

val Scala212 = "2.12.19"
val Scala213 = "2.13.13"

// Spark version to build against. Defaults to the Spark 3.5 line; override with
//   sbt -Dspark.version=4.1.2 ...
// Spark 4.x dropped Scala 2.12 and needs Java 17+, so a Spark 4 build collapses the
// cross-build to Scala 2.13 only. The few Spark-version-specific bits live in
// nats-spark-connector/src/main/scala-spark{3,4}/ (see `SparkShim`).
val sparkVersion = sys.props.getOrElse("spark.version", "3.5.1")
val isSpark4 = sparkVersion.startsWith("4.")
val natsVersion = "2.22.0"
val munitVersion = "0.7.29"

ThisBuild / crossScalaVersions := (if (isSpark4) Seq(Scala213) else Seq(Scala212, Scala213))
ThisBuild / scalaVersion := (if (isSpark4) Scala213 else Scala212) // "the" default Scala

ThisBuild / semanticdbEnabled := true // enable SemanticDB
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision // only required for Scala 2.x

// TODO(@Marcus-Rosti): we HAVE to deploy to maven, I just don't know exactly how
publishTo := None

// TODO(@Marcus-Rosti): build the other connector styles here
lazy val root = (project in file("."))
  .aggregate(`nats-spark-connector`)
  .settings(
    publish := false
  )

lazy val `nats-spark-connector` = (project in file("nats-spark-connector")).settings(
  // Spark 4 artifacts get a distinct name so the two Scala 2.13 jars can't be confused:
  //   nats-spark-connector_2.13-x.y.z.jar        (Spark 3.5)
  //   nats-spark-connector-spark4_2.13-x.y.z.jar (Spark 4)
  name := (if (isSpark4) "nats-spark-connector-spark4" else "nats-spark-connector"),
  Compile / unmanagedSourceDirectories +=
    (Compile / sourceDirectory).value / (if (isSpark4) "scala-spark4" else "scala-spark3"),
  Compile / scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12))  => List("-Ywarn-unused-import")
      case Some((2, 13)) => List("-Wunused:imports")
      case _ => Nil
    }
  },
  // TODO(@Marcus-Rosti): fix all of these
  Compile / compile / wartremoverErrors ++= Warts.allBut(
      Wart.Null,
      Wart.Equals,
      Wart.NonUnitStatements,
      Wart.Throw,
      Wart.Overloading,
      Wart.Any,
      Wart.StringPlusAny
    ),
    libraryDependencies ++= Seq(
    // TODO(@Marcus-Rosti): Maybe we shouldn't require this, more of a BYO-jnats?
    "io.nats" % "jnats" % natsVersion,
  ) ++ Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-unsafe" % sparkVersion,
    "org.apache.spark" %% "spark-catalyst" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-sql-api" % sparkVersion,
    "org.apache.spark" %% "spark-common-utils" % sparkVersion,
  ).map(_ % Provided) ++
    Seq(
      "org.scalameta" %% "munit" % munitVersion
    ).map(_ % Test),
  // Spark always ships its own scala-library; bundling another copy in the fat jar only
  // invites NoSuchMethodErrors when the two versions differ (e.g. with userClassPathFirst).
  assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("shapeless.**" -> "nats_spark_internal.@1").inAll,
    ShadeRule.rename("cats.kernel.**" -> s"nats_spark_internal.kernel.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) =>
      xs.map(_.toLowerCase) match {
        case ps @ (x :: xs) if ps.exists(_.endsWith(".sf")) => MergeStrategy.discard
        case ps @ (x :: xs) if ps.exists(_.endsWith(".dsa")) => MergeStrategy.discard
        case ps @ (x :: xs) if ps.exists(_.endsWith(".rsa")) => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case x => (assembly / assemblyMergeStrategy).value(x)
  }
)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
  runClean,                               // : ReleaseStep
  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  publishArtifacts,                       // : ReleaseStep, checks whether `publishTo` is properly set up
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)