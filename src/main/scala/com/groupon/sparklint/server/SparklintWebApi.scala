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

package com.groupon.sparklint.server

import java.nio.file.{Files, Paths}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, IsoFields, TemporalUnit}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, FSM}
import akka.pattern._
import akka.util.Timeout
import com.groupon.sparklint.actors.{ExecutorSink, LifeCycleSink, SparklintAppLogReader, VersionSink}
import org.apache.spark.groupon.StringToSparkEvent
import org.http4s.dsl._
import org.http4s.{HttpService, Response, UrlForm}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.pretty

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Task

/**
  * @author rxue
  * @since 11/22/17.
  */
trait SparklintWebApi {
  this: AdhocServer =>
  registerService("/sparklintApi", sparklintWebApi)

  case class LogReaderInfo(description: String, ref: ActorRef)

  private val logReaders: mutable.Map[String, LogReaderInfo] = mutable.Map.empty

  def sparklintWebApi = HttpService {
    case GET -> Root =>
      jsonResponse(Ok(pretty("readers" -> logReaders.map {
        case (readerUuid, readerInfo) =>
          ("uuid" -> readerUuid) ~
            ("description" -> readerInfo.description)
      })))
    case req@POST -> Root / "readLocalFile" =>
      req.decode[UrlForm] { form =>
        form.getFirst("uri") match {
          case Some(path) if Files.exists(Paths.get(path)) =>
            val file = Source.fromFile(path).getLines().flatMap(StringToSparkEvent.apply)
            val uuid = UUID.randomUUID().toString
            val reader = actorSystem.actorOf(SparklintAppLogReader.props(uuid, file))
            val description = s"file://$path"
            logReaders(uuid) = LogReaderInfo(description, reader)
            reader ! SparklintAppLogReader.StartInitializing
            reader ! SparklintAppLogReader.ReadTillEnd
            jsonResponse(Ok(pretty(("uuid" -> uuid) ~ ("description" -> description))))
          case Some(badPath) =>
            jsonResponse(BadRequest(pretty("message" -> s"file not found: $badPath")))
          case None =>
            jsonResponse(BadRequest(pretty("message" -> s"need 'uri' field with local file path as content")))
        }
      }
    case GET -> Root / uuid if logReaders.contains(uuid) =>
      implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      val logReader = logReaders(uuid).ref
      actorCall(logReader ? SparklintAppLogReader.ReportStatus, {
        case FSM.State(statename, SparklintAppLogReader.ProgressData(numRead), _, _, _) =>
          jsonResponse(Ok(pretty(("state" -> statename.toString) ~ ("recordsRead" -> numRead))))
      })
    case GET -> Root / uuid / query if logReaders.contains(uuid) =>
      implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      val logReader = logReaders(uuid).ref
      query match {
        case "version" =>
          actorCall(logReader ? VersionSink.GetVersion, {
            case VersionSink.VersionResponse(version) =>
              jsonResponse(Ok(pretty("version" -> version)))
          })
        case "liveExecutors" =>
          actorCall(logReader ? ExecutorSink.GetLiveExecutors, {
            case ExecutorSink.ExecutorsResponse(executors) =>
              jsonResponse(Ok(pretty(renderExecutors(executors))))
          })
        case "allExecutors" =>
          actorCall(logReader ? ExecutorSink.GetAllExecutors, {
            case r@ExecutorSink.ExecutorsResponse(executors) =>
              jsonResponse(Ok(pretty(renderExecutors(executors) ~ ("availableCores" -> r.cores))))
          })
        case "deadExecutors" =>
          actorCall(logReader ? ExecutorSink.GetDeadExecutors, {
            case ExecutorSink.ExecutorsResponse(executors) =>
              jsonResponse(Ok(pretty(renderExecutors(executors))))
          })
        case "appState" =>
          actorCall(logReader ? LifeCycleSink.GetLifeCycle, {
            case r: LifeCycleSink.LifeCycleResponse =>
              jsonResponse(Ok(pretty(("state" -> r.state) ~
                ("startedAt" -> r.started) ~
                ("startedTime" -> r.started.map(prettyPrintTime)) ~
                ("finishedAt" -> r.finished) ~
                ("finishedTime" -> r.finished.map(prettyPrintTime)) ~
                ("durationMs" -> r.duration) ~
                ("duration" -> r.duration.map(prettyPrintDuration)))))
          })
      }
  }

  private def renderExecutors(executors: Map[String, ExecutorSink.ExecutorSummary]): JObject = {
    "executors" -> executors.map({
      case (executorId, executorSummary) =>
        ("executorId" -> executorId) ~
          ("state" -> (if (executorSummary.alive) "Alive" else "Dead")) ~
          ("addedAt" -> executorSummary.added) ~
          ("addedTime" -> prettyPrintTime(executorSummary.added)) ~
          ("cores" -> executorSummary.cores) ~
          ("removedAt" -> executorSummary.removed) ~
          ("removedTime" -> executorSummary.removed.map(prettyPrintTime))
    })
  }

  private def actorCall(actorAsk: Future[Any], onSuccess: PartialFunction[Any, Task[Response]]): Task[Response] = {
    val handleUnexpectedResponse: PartialFunction[Any, Task[Response]] = {
      case unexpected =>
        jsonResponse(InternalServerError(pretty("message" -> s"unexpected response $unexpected")))
    }
    Try(Await.result(actorAsk, Duration.Inf)) match {
      case Success(response) =>
        onSuccess.orElse(handleUnexpectedResponse).apply(response)
      case Failure(ex) =>
        jsonResponse(BadRequest(pretty("message" -> ex.getMessage)))
    }
  }

  private def prettyPrintTime(time: Long): String = {
    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(time))
  }

  private def prettyPrintDuration(duration: Long): String = {
    java.time.Duration.ofMillis(duration).toString.substring(2)
      .replaceAll("(\\d[HMS])(?!$)", "$1 ")
      .toLowerCase()
  }

}
