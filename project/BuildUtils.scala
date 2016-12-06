import sbt.complete.DefaultParsers._
import sbt.complete.Parser
import sbt.{Command, Help, State}

object BuildUtils {
  private val SPARK_1_6_VERSIONS = Seq("1.6.0", "1.6.1", "1.6.2", "1.6.3")
  private val SPARK_2_0_VERSIONS = Seq("2.0.0", "2.0.1", "2.0.2")

  val SUPPORTED_SPARK_VERSIONS: Seq[String] = SPARK_1_6_VERSIONS ++ SPARK_2_0_VERSIONS

  private val uber_command_text = "foreachSparkVersion"
  private val uber_command_help = Help(
    Seq(s"$uber_command_text <extra tasks>" -> "foreach supported spark version, perform a series of task"),
    Map(uber_command_text ->
      """Perform tasks foreach supported spark version. This will:
        |1. Enumerate all supported version in BuildUtils.SUPPORTED_SPARK_VERSIONS
        |2. execute `set sparkVersion := "<version>"` and any extra tasks
      """.stripMargin))

  def uber_command_parser(state: State): Parser[String] = {
    Space ~> token(StringBasic, "<extra tasks>")
  }

  def uber_command_action(state: State, extraTasks: String): State = {
    SUPPORTED_SPARK_VERSIONS.foldRight(state) { (version, state) =>
      "set sparkVersion := \"" + version + "\"" :: extraTasks :: state
    }
  }

  val foreachSparkVersion: Command = Command(uber_command_text, uber_command_help)(uber_command_parser)(uber_command_action)

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
