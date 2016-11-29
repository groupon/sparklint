import de.heikoseeberger.sbtheader.license.Apache2_0

organization := "com.groupon.sparklint"
name := "sparklint"

description := "Listener and WebUI for Apache Spark performance events"
homepage := Some(url("https://github.com/groupon/sparklint"))
startYear := Some(2016)
licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

val mainClassName = "com.groupon.sparklint.SparklintServer"

/* ----- Dependencies ---------*/
resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("snapshot"),
  Resolver.sonatypeRepo("releases")
)

val spark = "2.0.2"
val http4s = "0.13.2"
val optparse = "1.1.2"
val scalatest = "2.2.6"
val json4s = "3.2.11"
val slf4j = "1.7.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.http4s" %% "http4s-dsl" % http4s,
  "org.http4s" %% "http4s-blaze-server" % http4s,
  "com.frugalmechanic" %% "scala-optparse" % optparse,
  "org.json4s" %% "json4s-native" % json4s,
  "org.slf4j" % "slf4j-api" % slf4j,
  "org.slf4j" % "slf4j-log4j12" % slf4j % "runtime",
  "log4j" % "log4j" % "1.2.17" % "runtime",
  "org.scalatest" %% "scalatest" % scalatest % "test",
  "org.http4s" %% "http4s-blaze-client" % http4s % "test"
)

/* ----- Testing ---------*/
// exclude UIPreview tag from test executions
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "UIPreview")

/* ----- Run ---------*/
mainClass in (Compile, run) := Some(mainClassName)
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

/* ----- Package ---------*/
publishMavenStyle := true
publishArtifact in Test := false
publishArtifact in (Compile, packageDoc) := false
publishArtifact in (Compile, packageSrc) := false
mainClass in (Compile, packageBin) := Some(mainClassName)

headers := Map(
  "scala" -> Apache2_0(startYear.value.get.toString, "Groupon, Inc.")
)

// To sync with Maven central
pomExtra in Global := {
    <scm>
      <connection>scm:git:git://github.com/groupon/sparklint.git</connection>
      <developerConnection>scm:git:ssh://git@github.com/groupon/sparklint.git</developerConnection>
      <url>github.com/groupon/sparklint.git</url>
    </scm>
    <developers>
      <developer>
        <name>Robert Xue</name>
        <email>rxue@groupon.com</email>
        <organization>Groupon, Inc.</organization>
        <organizationUrl>http://www.groupon.com</organizationUrl>
      </developer>
      <developer>
        <name>Simon Whitear</name>
        <email>swhitear@groupon.com</email>
        <organization>Groupon, Inc.</organization>
        <organizationUrl>http://www.groupon.com</organizationUrl>
      </developer>
    </developers>
}
