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
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, PoisonPill}
import akka.pattern._
import akka.util.Timeout
import com.groupon.sparklint.actors._
import org.apache.spark.groupon.{SparkEventToJson, StringToSparkEvent}
import org.http4s.dsl._
import org.http4s.{HttpService, Request, Response, UrlForm}
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

  def sparklintWebApi = HttpService(logManagementApi orElse queryApi)

  def registerLogReceiver(id: String, receiverDescription: String): ActorRef = {
    val ref = actorSystem.actorOf(SparklintEventLogReceiver.props(id))
    logReaders(id) = LogReaderInfo(receiverDescription, ref)
    ref
  }

  private def logManagementApi: PartialFunction[Request, Task[Response]] = {
    case GET -> Root / "readers" =>
      jsonResponse(Ok(pretty("readers" -> logReaders.map {
        case (readerId, readerInfo) =>
          ("id" -> readerId) ~
            ("description" -> readerInfo.description)
      })))
    case req@POST -> Root / "reader" =>
      req.decode[UrlForm] { form =>
        // lazy: Option[Boolean] -> lazy read
        // uri: String -> uri to log file
        // id: Option[String] -> specify id
        form.getFirst("uri") match {
          case Some(path) if Files.exists(Paths.get(path)) =>
            val file = Source.fromFile(path).getLines().flatMap(StringToSparkEvent.apply)
            val id = form.getFirst("id").getOrElse(UUID.randomUUID().toString)
            val reader = actorSystem.actorOf(SparklintAppLogReader.props(id, file))
            val description = s"file://$path"
            logReaders(id) = LogReaderInfo(description, reader)
            val lazyRead = form.getFirst("lazy").flatMap(s => Try(s.toBoolean).toOption).getOrElse(false)
            reader ! SparklintAppLogReader.StartInitializing
            if (!lazyRead) {
              reader ! SparklintAppLogReader.ResumeReading
            }
            jsonResponse(Ok(pretty(("id" -> id) ~ ("description" -> description))))
          case Some(badPath) =>
            jsonResponse(BadRequest(pretty("message" -> s"file not found: $badPath")))
          case None =>
            jsonResponse(BadRequest(pretty("message" -> s"need 'uri' field with local file path as content")))
        }
      }
    case GET -> Root / "reader" / id if logReaders.contains(id) =>
      implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      val logReader = logReaders(id).ref
      actorCall(logReader ? SparklintAppLogReader.GetReaderStatus, {
        case SparklintAppLogReader.GetReaderStatusResponse(statename, SparklintAppLogReader.ProgressData(numRead), lastRead) =>
          jsonResponse(Ok(pretty(("state" -> statename.toString) ~
            ("recordsRead" -> numRead) ~
            ("lastRecord" -> lastRead.map(SparkEventToJson.apply)))))
      })
    case PUT -> Root / "reader" / id / command if logReaders.contains(id) =>
      implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      val logReader = logReaders(id).ref
      val sendCommand: PartialFunction[String, Unit] = {
        case "pause" =>
          logReader ! SparklintAppLogReader.PauseReading
        case "readLine" =>
          logReader ! SparklintAppLogReader.ReadNextLine
        case "resume" =>
          logReader ! SparklintAppLogReader.ResumeReading
      }
      val unknownCommand: PartialFunction[String, Task[Response]] = {
        case unrecognized =>
          jsonResponse(BadRequest(pretty("message" -> s"unrecognized command [$unrecognized]")))
      }
      val pipeline = sendCommand.andThen(_ => {
        actorCall(logReader ? SparklintAppLogReader.GetReaderStatus, {
          case SparklintAppLogReader.GetReaderStatusResponse(statename, SparklintAppLogReader.ProgressData(numRead), lastRead) =>
            jsonResponse(Ok(pretty(("state" -> statename.toString) ~
              ("recordsRead" -> numRead) ~
              ("lastRecord" -> lastRead.map(SparkEventToJson.apply)))))
        })
      }).orElse(unknownCommand)
      pipeline(command)
    case DELETE -> Root / "reader" / id =>
      logReaders.remove(id) match {
        case Some(logReaderInfo) =>
          logReaderInfo.ref ! PoisonPill
          jsonResponse(Ok(pretty("message" -> s"Deleted reader $id (${logReaderInfo.description})")))
        case None =>
          jsonResponse(NotFound(pretty("message" -> s"Reader $id not found")))
      }
  }

  private def queryApi: PartialFunction[Request, Task[Response]] = {
    case GET -> Root / id / query if logReaders.contains(id) =>
      implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      val logReader = logReaders(id).ref
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
        case unrecognized =>
          jsonResponse(BadRequest(pretty("message" -> s"unrecognized query [$unrecognized]")))
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
        jsonResponse(InternalServerError(pretty("message" -> s"unexpected response [$unexpected]")))
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
