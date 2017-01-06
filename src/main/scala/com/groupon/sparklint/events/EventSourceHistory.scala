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

package com.groupon.sparklint.events

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.ZipFile

import com.google.common.io.Files
import com.groupon.sparklint.common.Logging
import org.http4s._
import org.http4s.client.blaze._
import org.http4s.json4s.jackson._
import org.json4s.{DefaultFormats, FieldSerializer}
import org.xerial.snappy.SnappyInputStream

import scala.collection.mutable

class EventSourceHistory(
    eventSourceManager: FileEventSourceManager,
    val uri: Uri,
    val dir: File,
    runImmediately: Boolean
) extends Logging {

  implicit val logger: Logging = this

  private val api = new HistoryApi(uri)
  private var loadedApps = mutable.Set.empty[String]

  def poll(): Unit = {
    newApps.foreach { app =>
      loadedApps += app.id

      api.getLogs(app, dir).foreach { log =>
        eventSourceManager.addFile(log) match {
          case Some(fileSource) =>
            if (runImmediately) {
              fileSource.forwardIfPossible()
            }
          case None =>
            logger.logWarn(s"Failed to construct source from ${log.getName}")
        }
      }
    }
  }

  private def newApps: List[Application] = {
    api.applications.filterNot(app => loadedApps.contains(app.id))
  }
}

private case class Attempt(
    attemptId: Option[String],
    startTime: String,
    endTime: String,
    sparkUser: String,
    completed: Boolean
)

private case class Application(
    id: String,
    name: String,
    attempts: List[Attempt]
)

private class HistoryApi(uri: Uri)(implicit logger: Logging) {
  private implicit val formats = DefaultFormats +
      FieldSerializer[Application]() +
      FieldSerializer[Attempt]()

  private val httpClient = PooledHttp1Client()

  /** Base Spark History API URI. */
  private def apiUri = uri / "api" / "v1"

  /** Fetches and uncompresses all logs for a given application into dir. */
  def getLogs(app: Application, dir: File): List[File] = {
    val request = Request(uri = apiUri / "applications" / app.id / "logs")
    val archive = File.createTempFile(s"eventLogs-${app.id}", ".zip")
    httpClient.expect(request)(EntityDecoder.binFile(archive)).run

    val zf = new ZipFile(archive)
    try {
      app.attempts.flatMap {
        case Attempt(Some(attemptId), _, _, _, true) =>
          val basename = s"${app.id}_$attemptId"
          val entry = zf.getEntry(basename + ".snappy")
          // XXX the analyzer want uncompressed files, but I think it
          //     might be better to store them compressed.
          val is = new SnappyInputStream(zf.getInputStream(entry))
          val log = new File(dir, basename)
          Files.asByteSink(log).writeFrom(is)
          is.close()
          Some(log)
        case _ => None
      }
    } finally {
      zf.close()
      archive.delete()
    }
  }

  /** Returns a list of applications completed today. */
  def applications: List[Application] = {
    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val request = Request(uri = (apiUri / "applications")
        .withQueryParam("status", "completed")
        .withQueryParam("minDate", today))
    httpClient.expect(request)(jsonExtract[List[Application]]).run
  }

  def shutdownNow(): Unit = httpClient.shutdownNow()
}