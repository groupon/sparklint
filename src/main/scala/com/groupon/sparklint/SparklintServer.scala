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

package com.groupon.sparklint

import com.frugalmechanic.optparse.OptParse
import com.groupon.sparklint.common._
import com.groupon.sparklint.events.{DirectoryEventSourceManager, FileEventSource, RootEventSourceManager, SingleFileEventSourceManager}
import com.groupon.sparklint.ui.UIServer

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * @author rxue, swhitear
  * @since 8/15/16.
  */
class SparklintServer(scheduler: SchedulerLike,
                      config: CliSparklintConfig)
  extends Logging {
  val eventSourceManager = new RootEventSourceManager()

  implicit val logger: Logging = this

  private val DEFAULT_RUN_IMMEDIATELY = false
  private val DEFAULT_POLL            = 5

  private var ui: Option[UIServer] = None

  /**
    * Main entry point for the server based version of SparkLint.
    */
  def startUI(): Unit = {
    shutdownUI()
    // wire up the front end server using the analyzer to adapt state via models
    val uiServer = new UIServer(eventSourceManager, config)
    ui = Some(uiServer)
    uiServer.startServer()
  }

  def shutdownUI(): Unit = {
    if (ui.isDefined) {
      logInfo(s"Shutting down...")
      ui.get.stopServer()
    }
  }

  def addEventSourcesFromCommandLineArguments(): Unit = {
    val runImmediately = config.runImmediately.getOrElse(DEFAULT_RUN_IMMEDIATELY)
    if (config.historySource) {
      logError("historySource unsupported.")
    }
    if (config.directorySource) {
      logInfo(s"Loading data from directory source ${config.directorySource.get}")
      val directorySource = DirectoryEventSourceManager(config.directorySource.get)
      eventSourceManager.addDirectory(directorySource)
      scheduleDirectoryPolling(directorySource)
    }
    if (config.fileSource) {
      logInfo(s"Loading data from file source ${config.fileSource.get}")
      Try(FileEventSource(config.fileSource.get)) match {
        case Success(fileSource) =>
          eventSourceManager.addFile(fileSource)
          if (runImmediately) fileSource.forwardIfPossible()
        case Failure(ex) => logger.logError(s"Failed to create file source from ${config.fileSource.get}, reason ${ex.getMessage}")
      }
    } else {
      logWarn("No source specified, add source through UI.")
    }
  }


  private def scheduleDirectoryPolling(directoryEventSource: DirectoryEventSourceManager) = {
    val taskName = s"Directory source poller [${directoryEventSource.dir.getName}]"
    val pollRate = config.pollRate.getOrElse(DEFAULT_POLL)
    val task = ScheduledTask(taskName, directoryEventSource.pull, periodSeconds = pollRate)
    scheduler.scheduleTask(task)
  }

  addEventSourcesFromCommandLineArguments()
}

object SparklintServer extends Logging with OptParse {
  def main(args: Array[String]): Unit = {
    val scheduler = new Scheduler()
    val config = CliSparklintConfig().parseCliArgs(args)
    val server = new SparklintServer(scheduler, config)
    server.startUI()
    waitForever
  }

  def waitForever: Future[Nothing] = {
    val p = Promise()
    Await.ready(p.future, Duration.Inf)
  }
}
