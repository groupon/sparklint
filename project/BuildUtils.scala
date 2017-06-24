import sbt.complete.DefaultParsers._
import sbt.complete.Parser
import sbt.{Command, Help, SettingKey, State}
import sbtrelease.Git
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, releaseVcs}
import sbtrelease.Utilities._

object BuildUtils {
  private val SPARK_1_6_VERSIONS = Seq("1.6.0", "1.6.1", "1.6.2", "1.6.3")
  private val SPARK_2_0_VERSIONS = Seq("2.0.0", "2.0.1", "2.0.2", "2.1.0", "2.1.1")

  val SUPPORTED_SPARK_VERSIONS: Seq[String] = SPARK_1_6_VERSIONS ++ SPARK_2_0_VERSIONS

  // Foreach Spark Version Command
  private val uberCommandText = "foreachSparkVersion"
  private val uberCommandHelp = Help(
    Seq(s"$uberCommandText <extra tasks>" -> "foreach supported spark version, perform a series of task"),
    Map(uberCommandText ->
      """Perform tasks foreach supported spark version. This will:
        |1. Enumerate all supported version in BuildUtils.SUPPORTED_SPARK_VERSIONS
        |2. execute `set sparkVersion := "<version>"` and any extra tasks
      """.stripMargin))

  def uberCommandParser(state: State): Parser[String] = {
    Space ~> token(StringBasic, "<extra tasks>")
  }

  def uberCommandAction(state: State, extraTasks: String): State = {
    SUPPORTED_SPARK_VERSIONS.foldRight(state) { (version, state) =>
      "set sparkVersion := \"" + version + "\"" :: extraTasks :: state
    }
  }

  val foreachSparkVersion: Command = Command(uberCommandText, uberCommandHelp)(uberCommandParser)(uberCommandAction)

  // One command release
  private val sparklintReleaseCommandText = "sparklintRelease"
  private val sparklintReleaseCommandHelp = Help(
    Seq(sparklintReleaseCommandText -> "release sparklint for every scala spark version, and to docker"),
    Map(sparklintReleaseCommandText ->
      """Release Sparklint:
        |1. Foreach spark version and scala version, publish jar to sonatype
        |2. Publish docker image
      """.stripMargin))

  val sparklintReleaseCommand: Command = Command.command(sparklintReleaseCommandText) {
    state =>
      "+ foreachSparkVersion publishSigned" :: "reload" :: "dockerBuildAndPush" :: state
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

  lazy val deployBranch: SettingKey[String] = SettingKey("deploy-branch", "Defines the name of the master branch to track the released versions")

  def mergeReleaseVersion: (State) => State = { st: State =>
    // Force using git
    val git: Git = st.extract.get(releaseVcs).get.asInstanceOf[Git]
    val mainBranch: String = st.extract.get(deployBranch)
    val currentBranch = (git.cmd("rev-parse", "--abbrev-ref", "HEAD") !!).trim
    st.log.info(s"Releasing from: [$currentBranch]. Check out [$mainBranch]")
    git.cmd("checkout", mainBranch) ! st.log
    st.log.success(s"Switched to $mainBranch")
    st.log.info(s"Pulling $mainBranch")
    git.cmd("pull") ! st.log
    st.log.success(s"Pulled $mainBranch")
    st.log.info(s"Merging $currentBranch to $mainBranch")
    git.cmd("merge", currentBranch, "--no-edit") ! st.log
    st.log.success(s"Merged $currentBranch to $mainBranch")
    st.log.info(s"Push to upstream")
    git.pushChanges ! st.log
    st.log.success(s"Pushed $mainBranch to upstream")
    st.log.info(s"Checkout $currentBranch")
    git.cmd("checkout", currentBranch) ! st.log
    st.log.success(s"Switched to $currentBranch")
    st
  }

}
