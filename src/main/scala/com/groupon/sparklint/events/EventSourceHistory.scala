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
import java.util.regex.Pattern
import java.util.zip.ZipFile

import com.google.common.io.Files
import com.groupon.sparklint.common.Logging
import org.http4s._
import org.http4s.client.blaze._
import org.http4s.json4s.jackson._
import org.json4s.{DefaultFormats, FieldSerializer}
import org.xerial.snappy.SnappyInputStream

import scala.collection.mutable
import scalaz.{-\/, \/-}

/**
 * This class fetches Spark application log files via Spark History API
 * and persists them in [[dir]].
 *
 * Only application with a name matching [[pattern]] will be considered.
 *
 * @author superbobry
 * @since 1/5/17.
 */
class EventSourceHistory(
    eventSourceManager: FileEventSourceManager,
    val uri: Uri,
    val dir: File,
    val pattern: Pattern,
    runImmediately: Boolean
) extends Logging {

  implicit val logger: Logging = this

  private val api = new HistoryApi(uri)
  private val loadedApps = mutable.Set.empty[String]

  def poll(): Unit = {
    val snapshot = loadedApps.size
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

      logger.logInfo(s"Loaded ${app.name}")
    }

    logger.logInfo(s"Loaded ${loadedApps.size - snapshot} new apps")
  }

  private def newApps: Seq[ApplicationHistoryInfo] = {
    api.getApplications()
        .filter(app => pattern.matcher(app.name).matches())
        .filterNot(app => loadedApps.contains(app.id))
  }
}

case class ApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: String,
    endTime: String,
    sparkUser: String,
    completed: Boolean = false)

case class ApplicationHistoryInfo(
    id: String,
    name: String,
    attempts: List[ApplicationAttemptInfo])

private class HistoryApi(uri: Uri)(implicit logger: Logging) {
  private implicit val formats = DefaultFormats +
      FieldSerializer[ApplicationHistoryInfo]() +
      FieldSerializer[ApplicationAttemptInfo]()

  private val httpClient = PooledHttp1Client()

  /** Base Spark History API URI. */
  private def apiUri = uri / "api" / "v1"

  /** Fetches and uncompresses all logs for a given application into dir. */
  def getLogs(app: ApplicationHistoryInfo, dir: File): List[File] = {
    val request = Request(uri = apiUri / "applications" / app.id / "logs")
    val archive = File.createTempFile(s"eventLogs-${app.id}", ".zip")

    httpClient.expect(request)(EntityDecoder.binFile(archive)).attemptRun match {
      case -\/(e) =>
        logger.logError(s"Failed to fetch logs for ${app.id}", e)
        List()
      case \/-(_) =>
        val zf = new ZipFile(archive)
        try {
          app.attempts.map { attempt: ApplicationAttemptInfo =>
            val basename = attempt.attemptId match {
              case Some(attemptId) => s"${app.id}_$attemptId"
              case None => app.id
            }

            val entry = zf.getEntry(basename + ".snappy")
            // XXX the analyzer wants uncompressed files, but I think it
            //     might be better to store them compressed.
            val is = new SnappyInputStream(zf.getInputStream(entry))
            val log = new File(dir, basename)
            Files.asByteSink(log).writeFrom(is)
            is.close()
            log
          }
        } finally {
          zf.close()
          archive.delete()
        }
    }
  }

  /** Returns a list of applications completed today. */
  def getApplications(minDate: Date = new Date()): List[ApplicationHistoryInfo] = {
    val request = Request(uri = (apiUri / "applications")
        .withQueryParam("status", "completed")
        .withQueryParam("minDate", new SimpleDateFormat("yyyy-MM-dd").format(minDate)))
    httpClient.expect(request)(jsonExtract[List[ApplicationHistoryInfo]]).run
  }

  def shutdownNow(): Unit = httpClient.shutdownNow()
}