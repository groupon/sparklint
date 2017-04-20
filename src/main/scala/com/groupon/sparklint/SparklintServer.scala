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
import org.http4s.Uri

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

/**
  * @author rxue, swhitear
  * @since 8/15/16.
  */
object SparklintServer extends Logging with OptParse {
  def main(args: Array[String]): Unit = {
    val config = CliSparklintConfig().parseCliArgs(args)
    val server = new Sparklint(config)
    if (config.historySource) {
      val uri = Uri.fromString(config.historySource.get).toOption.get
      server.backend.appendHistoryServer(uri.host.get.value, uri)
    }
    if (config.fileSource) {
      server.backend.appendSingleFileManager(config.fileSource.get)
    }
    if (config.directorySource) {
      server.backend.appendFolderManager(config.directorySource.get)
    }
    // register initial event sources
    server.startServer()
    waitForever
  }

  def waitForever: Future[Nothing] = {
    val p = Promise()
    Await.ready(p.future, Duration.Inf)
  }
}
