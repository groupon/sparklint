package com.groupon.sparklint.common

import java.io.File

import com.frugalmechanic.optparse.OptParse

/**
  * A simple wrapper around some build time specific configuration properties.
  *
  * @author rxue, swhitear
  * @since 9/12/16.
  */
case class SparklintConfig(exitOnError: Boolean = true) extends OptParse with Logging {
  // For testing we want OptParse to throw exceptions instead of calling System.exit
  override val optParseExitOnError: Boolean = exitOnError

  val fileSource = FileOpt(
    long = "file", short = 'f',
    desc = "Filename of an Spark event log source to use."
  )

  val directorySource = FileOpt(
    long = "directory", short = 'd',
    desc = "Directory of an Spark event log sources to use. Read in filename sort order.",
    validate = (input: File) => input.isDirectory
  )

  val runImmediately = BoolOpt(
    long = "runImmediately", short = 'r',
    desc = "Set the flag in order to run each buffer through to their end state on startup."
  )

  val historySource = StrOpt(desc = "Url of the Spark History Server to use.")

  val pollRate = IntOpt(
    long = "pollRate", short = 'p',
    desc = "The interval (in seconds) between polling for changes in directory and history event sources.",
    validate = (value) => value > 0
  )

  /**
    *
    * @param args the command line arguments
    * @return the config built
    */
  def parseCliArgs(args: Array[String]): SparklintConfig = {
    logInfo(s"Parsing args: ${args.mkString(" ")}")
    parse(args)
    logInfo(s"Parsed into: ${foundOpts.map(o => s"${o.long.get}").mkString(", ")}")
    this
  }

  // TODO: We need to support SparkConf based configuration as well to allow SparklintListener config this object
}
