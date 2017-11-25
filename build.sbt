import BuildUtils._
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbtdocker.DockerKeys.dockerBuildAndPush
import sbtdocker.ImageName
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._


// Meta
name := "sparklint"
description := "Listener and WebUI for Apache Spark performance events"
organization := "com.groupon.sparklint"
homepage := Some(url("https://github.com/groupon/sparklint"))
startYear := Some(2016)
licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

// Compile
enablePlugins(AutomateHeaderPlugin)
name := s"sparklint-spark${getProjectNameSuffix(sparkVersion.value)}"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")
unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / getSparkMajorVersion(sparkVersion.value)
unmanagedSourceDirectories in Test += (sourceDirectory in Test).value / getSparkMajorVersion(sparkVersion.value)
assemblyOption in assembly := (assemblyOption in assembly)
  .value
  .copy(includeScala = false)
// Dependency
// Spark
lazy val sparkVersion = SettingKey[String]("spark-version", "The version of spark library to compile against")
sparkVersion := "2.1.0"
// Non-spark
lazy val http4s = "0.15.5"
lazy val optparse = "1.1.2"
lazy val scalatest = "3.0.4"
lazy val slf4j = "1.7.16"
lazy val log4j = "1.2.17"
lazy val json4s = "3.2.11"
lazy val jackson = "2.6.5"
lazy val akkaVersion = "2.5.7"
lazy val akkaHttpVersion = "10.0.6"

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("snapshot"),
  Resolver.sonatypeRepo("releases")
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value
    exclude("com.fasterxml.jackson.module", "*"),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value
    exclude("com.fasterxml.jackson.module", "*"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion.value
    exclude("com.fasterxml.jackson.module", "*"),

  "com.frugalmechanic" %% "scala-optparse" % optparse,

  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http"  % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,

  "org.http4s" %% "http4s-dsl" % http4s,
  "org.http4s" %% "http4s-blaze-server" % http4s,
  "org.http4s" %% "http4s-json4s-jackson" % http4s
    exclude("org.json4s", "*"),
  "org.http4s" %% "http4s-blaze-client" % http4s,
  "org.slf4j" % "slf4j-api" % slf4j,
  "org.slf4j" % "slf4j-log4j12" % slf4j,
  "log4j" % "log4j" % log4j,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jackson,
  "org.json4s" %% "json4s-jackson" % json4s
)

// Run
mainClass in run := Some("com.groupon.sparklint.SparklintServer")
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run)).evaluated

// Test
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion,
  "org.scalatest" %% "scalatest" % scalatest
) map (_ % Test)
fork in Test := true

// Package fat jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Publish
publishMavenStyle := true
publishArtifact in Test := false

headers := Map(
  "scala" -> Apache2_0(startYear.value.get.toString, "Groupon, Inc.")
)

// Publish to Docker
enablePlugins(DockerPlugin)
// Tag the docker build as latest for snapshot, concrete version number for release
// FIXME: replace roboxue with groupon after organization account has been created
imageNames in docker := Seq(new ImageName(namespace = Some("roboxue"), repository = "sparklint",
  tag = if (!isSnapshot.value) Some(version.value) else Some("latest")))
dockerfile in docker := {
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("java")
    maintainer("Robert Xue", "roboxue@roboxue.com")
    add(artifact, artifactTargetPath)
    entryPoint("java", "-jar", artifactTargetPath)
    expose(23763)
  }
}

// Release customization
deployBranch := "master"
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("sparklintRelease"),
  releaseStepTask(dockerBuildAndPush),
  mergeReleaseVersion,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

// Package Multiple Spark Version
commands += foreachSparkVersion
// One command to release everything
commands += sparklintReleaseCommand

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
