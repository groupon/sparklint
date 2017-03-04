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
import java.util.zip.{ZipEntry, ZipInputStream}

import com.groupon.sparklint.common.Logging
import org.apache.spark.status.api.v1.ApplicationAttemptInfo
import org.http4s._
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.json4s.jackson._
import org.json4s.{DefaultFormats, FieldSerializer}
import org.xerial.snappy.SnappyInputStream

import scala.io.Source
import scalaz.concurrent.Task

/**
  * @author superbobry
  * @since 1/11/17.
  */
case class HistoryServerApi(name: String, uri: Uri) extends Logging {
  private implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
  } +
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
  def getLogs(appId: EventSourceIdentifier): (String, Task[Array[Char]]) = {
    val uri = appId.attemptId match {
      case Some(attemptId) =>
        apiUri / "applications" / appId.appId /attemptId / "logs"
      case None =>
        apiUri / "applications" / appId.appId / "logs"
    }
    val request = Request(uri = uri)
    val name = s"${appId.toString}.snappy"
    name -> httpClient.expect(request)(eventLogDecoder(name))
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

case class ApplicationHistoryInfo(id: String, name: String, attempts: List[ApplicationAttemptInfo])

