import de.heikoseeberger.sbtheader.license.Apache2_0

lazy val commonSettings = Seq(
  organization := "com.groupon.sparklint",
  homepage := Some(url("https://github.com/groupon/sparklint")),
  startYear := Some(2016),
  licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  scalaVersion := "2.10.6",
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  resolvers in ThisBuild ++= Seq(
    Resolver.sonatypeRepo("snapshot"),
    Resolver.sonatypeRepo("releases")
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatest % "test"
  ),

  publishMavenStyle := true,
  publishArtifact in Test := false,

  headers := Map(
    "scala" -> Apache2_0(startYear.value.get.toString, "Groupon, Inc.")
  ),

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
)

// non-spark library version
lazy val http4s = "0.13.2"
lazy val optparse = "1.1.2"
lazy val scalatest = "2.2.6"
lazy val slf4j = "1.7.16"
lazy val log4j = "1.2.17"
lazy val json4s = "3.2.11"

lazy val root = Project("sparklint", file("."))
  .dependsOn(sparklintUI, sparkConnector)
  .settings(commonSettings: _*)
  .settings(
    name := "sparklint",
    description := "Listener and WebUI for Apache Spark performance events",
    libraryDependencies ++= Seq(
      "com.frugalmechanic" %% "scala-optparse" % optparse,
      "org.http4s" %% "http4s-blaze-client" % http4s % "test"
    ),
    /* ----- Run ---------*/
    mainClass in(Compile, run) := Some("com.groupon.sparklint.SparklintServer")
  )

lazy val sparkConnector = project
  .dependsOn(sparklintUI)
  .settings(commonSettings: _*)
  .settings(
    description := "Utility library to convert spark to sparklint data structure"
  )

lazy val sparklintUI = project
  .settings(commonSettings: _*)
  .settings(
    name := "sparklint-ui",
    description := "UI components for sparklint that doesn't have spark dependencies",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-dsl" % http4s,
      "org.http4s" %% "http4s-blaze-server" % http4s,
      "org.slf4j" % "slf4j-api" % slf4j,
      "org.slf4j" % "slf4j-log4j12" % slf4j,
      "log4j" % "log4j" % log4j,
      "org.json4s" %% "json4s-jackson" % json4s
    )
  )
