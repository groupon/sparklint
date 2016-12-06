import de.heikoseeberger.sbtheader.license.Apache2_0

// Meta
name := "sparklint"
description := "Listener and WebUI for Apache Spark performance events"
organization := "com.groupon.sparklint"
homepage := Some(url("https://github.com/groupon/sparklint"))
startYear := Some(2016)
licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

// Compile
name:= s"sparklint-spark${BuildUtils.getProjectNameSuffix(sparkVersion.value)}"
scalaVersion := "2.10.6"
crossScalaVersions := Seq("2.10.6", "2.11.8")
unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / BuildUtils.getSparkMajorVersion(sparkVersion.value)
unmanagedSourceDirectories in Test += (sourceDirectory in Test).value / BuildUtils.getSparkMajorVersion(sparkVersion.value)

// Dependency
// Spark
lazy val sparkVersion = SettingKey[String]("spark-version", "The version of spark library to compile against")
sparkVersion := "1.6.1"
// Non-spark
lazy val http4s = "0.13.2"
lazy val optparse = "1.1.2"
lazy val scalatest = "2.2.6"
lazy val slf4j = "1.7.16"
lazy val log4j = "1.2.17"
lazy val json4s = "3.2.11"

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("snapshot"),
  Resolver.sonatypeRepo("releases")
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value,
  "com.frugalmechanic" %% "scala-optparse" % optparse,
  "org.http4s" %% "http4s-dsl" % http4s,
  "org.http4s" %% "http4s-blaze-server" % http4s,
  "org.slf4j" % "slf4j-api" % slf4j,
  "org.slf4j" % "slf4j-log4j12" % slf4j,
  "log4j" % "log4j" % log4j,
  "org.json4s" %% "json4s-jackson" % json4s,
  "org.scalatest" %% "scalatest" % scalatest % "test",
  "org.http4s" %% "http4s-blaze-client" % http4s % "test"
)

// Run
mainClass in run := Some("com.groupon.sparklint.SparklintServer")

// Package Multiple Spark Version
commands += BuildUtils.foreachSparkVersion

// Publish
publishMavenStyle := true
publishArtifact in Test := false

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
