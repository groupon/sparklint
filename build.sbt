import de.heikoseeberger.sbtheader.license.Apache2_0

organization := "com.groupon.sparklint"
name := "sparklint"

description := "Listener and WebUI for Apache Spark performance events"
homepage := Some(url("https://github.com/groupon/sparklint"))
startYear := Some(2016)
licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

scalaVersion := "2.10.6"
crossScalaVersions := Seq("2.10.6", "2.11.8")

/* ----- Dependencies ---------*/
resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("snapshot"),
  Resolver.sonatypeRepo("releases")
)

val spark = "1.6.1"
val http4s = "0.13.2"
val optparse = "1.1.2"
val scalatest = "2.2.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.http4s" %% "http4s-dsl" % http4s,
  "org.http4s" %% "http4s-blaze-server" % http4s,
  "com.frugalmechanic" %% "scala-optparse" % optparse,
  "org.scalatest" %% "scalatest" % scalatest % "test",
  "org.http4s" %% "http4s-blaze-client" % http4s % "test"
)

/* ----- Testing ---------*/
// exclude UIPreview tag from test executions
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "UIPreview")

/* ----- Run ---------*/
mainClass in (Compile, run) := Some("com.groupon.sparklint.SparklintServer")

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
