/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.common

import java.io.File

import com.frugalmechanic.optparse.OptParse
import org.http4s.Uri

/**
  * A simple wrapper around some build time specific configuration properties.
  *
  * @author rxue, swhitear
  * @since 9/12/16.
  */
case class CliSparklintConfig(exitOnError: Boolean = true) extends SparklintConfig
  with OptParse with Logging {
  // For testing we want OptParse to throw exceptions instead of calling System.exit
  override val optParseExitOnError: Boolean = exitOnError


  override def port: Int = portConfig.getOrElse(defaultPort)

  val portConfig = IntOpt(
    long = "port",
    desc = "Set the port for sparklint UI to launch"
  )

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

  val historySource: StrOpt = StrOpt(
    long = "historySource",
    desc = "Url of the Spark History Server to use.",
    validate = input => Uri.fromString(input).isLeft
  )

  val historyDir: FileOpt = FileOpt(
    long = "historyDir",
    desc = "Path to the directory for logs downloaded from Spark History Server.",
    validate = input => input.isDirectory,
    validWith = Seq(historySource)
  )

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
  def parseCliArgs(args: Array[String]): CliSparklintConfig = {
    logInfo(s"Parsing args: ${args.mkString(" ")}")
    parse(args)
    logInfo(s"Parsed into: ${foundOpts.map(o => s"${o.long.get}").mkString(", ")}")
    this
  }
}
