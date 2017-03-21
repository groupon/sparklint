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

import java.io.BufferedInputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern
import java.util.zip.{ZipEntry, ZipInputStream}

import com.groupon.sparklint.common.Logging
import org.http4s._
import org.http4s.client.blaze._
import org.http4s.json4s.jackson._
import org.json4s.{DefaultFormats, FieldSerializer}
import org.xerial.snappy.SnappyInputStream

import scala.collection.mutable
import scala.io.Source
import scalaz.concurrent.Task

/**
  * This class fetches Spark application log files via Spark History API.
  *
  * Only application with a name matching [[pattern]] will be considered.
  *
  * @author superbobry
  * @since 1/5/17.
  */
class EventSourceHistory(
    eventSourceManager: FileEventSourceManager,
    val uri: Uri,
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

      api.getLogs(app).foreach { case (name, task) =>
        eventSourceManager.addRemoteFile(name, task) match {
          case Some(fileSource) =>
            if (runImmediately) {
              fileSource.forwardIfPossible()
            }
          case None =>
            logger.logWarn(s"Failed to construct source from $name")
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

  private def eventLogDecoder(name: String): EntityDecoder[Array[Char]] =
    EntityDecoder.decodeBy(MediaRange.fromKey("*")) { msg =>
      val is = scalaz.stream.io.toInputStream(msg.body)
      val zis = new ZipInputStream(new BufferedInputStream(is))
      var entry: ZipEntry = null
      do {
        zis.closeEntry()
        entry = zis.getNextEntry
      } while (zis.available() > 0 && entry.getName != name)

      if (entry == null) {
        DecodeResult.failure(
          MalformedMessageBodyFailure(s"$name not found in ZIP archive"))
      } else {
        val sis = new SnappyInputStream(zis)
        try {
          // TODO: we could get rid of the explicit allocation if we move
          //       event parsing here.
          DecodeResult.success(Source.fromInputStream(sis).toArray)
        } finally {
          zis.closeEntry()
          sis.close()
        }
      }
    }

  /**
    * Fetches the event log for a given app and attempt.
    */
  def getLogs(app: ApplicationHistoryInfo): List[(String, Task[Array[Char]])] = {
    app.attempts.map { attempt =>
      val request = Request(uri = apiUri / "applications" / app.id / "logs")
      val name = attempt.attemptId match {
        case Some(attemptId) => s"${app.id}_$attemptId.snappy"
        case None            => s"${app.id}.snappy"
      }

      name -> httpClient.expect(request)(eventLogDecoder(name))
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