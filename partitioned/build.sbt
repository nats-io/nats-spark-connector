
name := "nats-spark-connector"
version := "1.1.2"
scalaVersion := "2.12.14"

val sparkVersion = "3.3.0"
// val sparkVersion = "2.4.5"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val slf4jVersion = "2.0.3"


//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.14"
libraryDependencies += "io.nats" % "jnats" % "2.13.2"

resolvers ++= Seq(
  "MavenRepository2" at "https://mvnrepository.com",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  //"com.holdenkarau" %% "spark-testing-base" % sparkTestingVersion % "test" ,
)
//).map(_.exclude("org.slf4j", "*"))
 
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.11"

libraryDependencies ++= Seq(
  //"org.slf4j" % "slf4j-api" % slf4jVersion % "provided",
  //"org.slf4j" % "slf4j-reload4j" % slf4jVersion % "provided",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.1",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.13.1",
)

 

// You can use Scaladex, an index of all known published Scala libraries. There,
// after you find the library you want, you can just copy/paste the dependency
// information that you need into your build file. For example, on the
// scala/scala-parser-combinators Scaladex page,
// https://index.scala-lang.org/scala/scala-parser-combinators, you can copy/paste
// the sbt dependency from the sbt box on the right-hand side of the screen.

// IMPORTANT NOTE: while build files look _kind of_ like regular Scala, it's
// important to note that syntax in *.sbt files doesn't always behave like
// regular Scala. For example, notice in this build file that it's not required
// to put our settings into an enclosing object or class. Always remember that
// sbt is a bit different, semantically, than vanilla Scala.

// ============================================================================

// Most moderately interesting Scala projects don't make use of the very simple
// build file style (called "bare style") used in this build.sbt file. Most
// intermediate Scala projects make use of so-called "multi-project" builds. A
// multi-project build makes it possible to have different folders which sbt can
// be configured differently for. That is, you may wish to have different
// dependencies or different testing frameworks defined for different parts of
// your codebase. Multi-project builds make this possible.

// Here's a quick glimpse of what a multi-project build looks like for this
// build, with only one "subproject" defined, called `root`:

// lazy val root = (project in file(".")).
//   settings(
//     inThisBuild(List(
//       organization := "ch.epfl.scala",
//       scalaVersion := "2.13.3"
//     )),
//     name := "hello-world"
//   )

// To learn more about multi-project builds, head over to the official sbt
// documentation at http://www.scala-sbt.org/documentation.html
