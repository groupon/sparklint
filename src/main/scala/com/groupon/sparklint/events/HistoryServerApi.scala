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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.ZipInputStream

import com.groupon.sparklint.common.Logging
import org.http4s._
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.json4s.jackson._
import org.json4s.{DefaultFormats, FieldSerializer}

import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Task
import scalaz.stream.io.toInputStream

/**
  * @author superbobry
  * @since 1.0.5
  */
case class HistoryServerApi(name: String, host: Uri) extends Logging {
  private implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
  } +
    FieldSerializer[ApplicationHistoryInfo]() +
    FieldSerializer[ApplicationAttemptInfo]()

  private val httpClient = PooledHttp1Client()

  /**
    * Fetches the event log for a given app and attempt.
    */
  def getLogs(uuid: String, appMeta: EventSourceMeta): Task[FreeScrollEventSource] = {
    val uri = appMeta.attempt match {
      case Some(attemptId) =>
        apiUri / "applications" / appMeta.appId.get / attemptId / "logs"
      case None =>
        apiUri / "applications" / appMeta.appId.get / "logs"
    }
    val request = Request(uri = uri)
    httpClient.expect(request)(eventLogDecoder(uuid, appMeta.appId.get))
  }

  private def eventLogDecoder(uuid: String, appId: String): EntityDecoder[FreeScrollEventSource] =
    EntityDecoder.decodeBy(MediaRange.fromKey("*")) { msg =>
      val zis = new ZipInputStream(toInputStream(msg.body))
      Try {
        EventSource.fromZipStream(zis, uuid, appId)
      } match {
        case Success(es) =>
          zis.closeEntry()
          zis.close()
          DecodeResult.success(es)
        case Failure(ex) =>
          zis.closeEntry()
          zis.close()
          DecodeResult.failure({
            MalformedMessageBodyFailure(ex.getMessage)
          })
      }
    }

  /** Base Spark History API URI. */
  private def apiUri = host / "api" / "v1"

  /** Returns a list of applications completed today. */
  def getApplications(minDate: Date = new Date()): List[ApplicationHistoryInfo] = {
    val request = Request(uri = (apiUri / "applications")
      .withQueryParam("status", "completed")
      .withQueryParam("minDate", new SimpleDateFormat("yyyy-MM-dd").format(minDate)))
    httpClient.expect(request)(jsonExtract[List[ApplicationHistoryInfo]]).run
  }

  def shutdownNow(): Unit = httpClient.shutdownNow()
}

case class ApplicationHistoryInfo(id: String, name: String, attempts: List[ApplicationAttemptInfo])

/**
  * A clone of the Spark's [[ApplicationAttemptInfo]], to keep the minimum set of information between version
  * So a sparklint for spark 2.0.1 can still parse the response from a history server with spark 1.6.1
  */
case class ApplicationAttemptInfo(attemptId: Option[String],
                                  startTime: Date,
                                  endTime: Date,
                                  sparkUser: String,
                                  completed: Boolean = false) {
  def getStartTimeEpoch: Long = startTime.getTime

  def getEndTimeEpoch: Long = endTime.getTime
}

