import sbt.{Command, Help}

object BuildUtils {
  private val SPARK_1_6_VERSIONS = Seq("1.6.0", "1.6.1", "1.6.2", "1.6.3")
  private val SPARK_2_0_VERSIONS = Seq("2.0.0", "2.0.1", "2.0.2")

  val SUPPORTED_SPARK_VERSIONS: Seq[String] = SPARK_1_6_VERSIONS ++ SPARK_2_0_VERSIONS

  private val PACKAGE_SPARKLINT = "packageSparklint"
  private val packageSparklintHelp        = Help(
    Seq(PACKAGE_SPARKLINT -> "package one jar for each supported spark version"),
    Map(PACKAGE_SPARKLINT ->
      """Package one jar for each supported spark version. This will:
        |1. Enumerate all supported version in BuildUtils.SUPPORTED_SPARK_VERSIONS
        |2. execute `set sparkVersion := "<version>" package`
      """.stripMargin))

  val packageSparklint: Command = Command.command(PACKAGE_SPARKLINT, packageSparklintHelp) {
    state =>
      SUPPORTED_SPARK_VERSIONS.foldRight(state) { (version, state) =>
        "set sparkVersion := \"" + version + "\"" :: "package" :: state
      }
  }

  def getProjectNameSuffix(versionString: String): String = versionString.replaceAll("[-\\.]", "")

  def getSparkMajorVersion(versionString: String): String = versionString match {
    case v if SPARK_1_6_VERSIONS.contains(v) =>
      "spark-1.6"
    case v if SPARK_2_0_VERSIONS.contains(v) =>
      "spark-2.0"
    case _                                   =>
      throw new UnsupportedOperationException(s"Spark version $versionString has not been supported yet")
  }
}
