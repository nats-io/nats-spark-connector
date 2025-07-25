import ReleaseTransformations._

ThisBuild / version := "2.1.7"

name := "nats-spark-connector"

organization := "io.nats"
organizationName := "nats"
startYear := Some(2024)
licenses := Seq(License.Apache2)

val Scala212 = "2.12.19"
val Scala213 = "2.13.13"

ThisBuild / crossScalaVersions := Seq(Scala212, Scala213)
ThisBuild / scalaVersion := Scala212 // "the" default Scala

ThisBuild / semanticdbEnabled := true // enable SemanticDB
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision // only required for Scala 2.x

// TODO(@Marcus-Rosti): we HAVE to deploy to maven, I just don't know exactly how
publishTo := None

val sparkVersion = "3.5.1"
val natsVersion = "2.21.4"
val munitVersion = "0.7.29"

// TODO(@Marcus-Rosti): build the other connector styles here
lazy val root = (project in file("."))
  .aggregate(`nats-spark-connector`)
  .settings(
    publish := false
  )

lazy val `nats-spark-connector` = (project in file("nats-spark-connector")).settings(
  name := "nats-spark-connector",
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