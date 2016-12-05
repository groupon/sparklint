object BuildUtils {
  def getProjectNameSuffix(versionString: String): String = versionString.replaceAll("[-\\.]", "")

  def getSparkMajorVersion(versionString: String): String = versionString match {
    case "1.6.0" | "1.6.1" | "1.6.2" | "1.6.3" =>
      "spark-1.6"
    case "2.0.0" | "2.0.1" | "2.0.2"           =>
      "spark-2.0"
    case _                                     =>
      throw new UnsupportedOperationException(s"Spark version $versionString has not been supported yet")
  }
}
