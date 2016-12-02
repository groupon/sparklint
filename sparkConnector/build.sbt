
lazy val sparkVersion = SettingKey[String]("spark-version", "The version of spark library to compile against")
sparkVersion := "1.6.1"
name := s"sparklint-connector-${getProjectNameSuffix(sparkVersion.value)}"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value
)

def getProjectNameSuffix(versionString: String): String = versionString.replaceAll("[-\\.]", "")

def getSparkMajorVersion(versionString: String): String = versionString match {
  case "1.6.0" | "1.6.1" | "1.6.2" | "1.6.3" =>
    "spark-1.6"
  case "2.0.0" | "2.0.1" | "2.0.2"           =>
    "spark-2.0"
  case _                                     =>
    throw new UnsupportedOperationException(s"Spark version $versionString has not been supported yet")
}

unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / getSparkMajorVersion(sparkVersion.value)
unmanagedSourceDirectories in Test += (sourceDirectory in Test).value / getSparkMajorVersion(sparkVersion.value)
