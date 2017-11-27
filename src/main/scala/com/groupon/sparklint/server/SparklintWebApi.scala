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
import com.groupon.sparklint.data.StorageOption
import org.apache.spark.groupon.{SparkEventToJson, StringToSparkEvent}
import org.http4s.dsl._
import org.http4s.{HttpService, Request, Response, UrlForm}
import org.json4s.JsonAST.{JField, JObject, JString, JValue}
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

  def registerLogReceiver(id: String, receiverDescription: String, storageOption: StorageOption): ActorRef = {
    val ref = actorSystem.actorOf(SparklintEventLogReceiver.props(id, storageOption))
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
        // metricsCapacity: Option[Long] -> how many data to keep, None for unlimited
        // metricsPruneFrequency: Option[Long] -> how often to retire old data, only works with metricsCapacity
        form.getFirst("uri") match {
          case Some(path) if Files.exists(Paths.get(path)) =>
            val file = Source.fromFile(path).getLines().flatMap(StringToSparkEvent.apply)
            val id = form.getFirst("id").getOrElse(UUID.randomUUID().toString)
            val storageOption = Try({
              val capacity = form.getFirst("metricsCapacity").get.toLong
              val pruneFrequency = Try(form.getFirst("metricsPruneFrequency").get.toLong).getOrElse(capacity)
              StorageOption.FixedCapacity(capacity, pruneFrequency)
            }).getOrElse(StorageOption.Lossless)
            val reader = actorSystem.actorOf(SparklintAppLogReader.props(id, file, storageOption))
            val description = s"file://$path"
            logReaders(id) = LogReaderInfo(description, reader)
            val lazyRead = form.getFirst("lazy").flatMap(s => Try(s.toBoolean).toOption).getOrElse(false)
            reader ! SparklintAppLogReader.StartInitializing
            if (!lazyRead) {
              reader ! SparklintAppLogReader.ResumeReading
            }
            jsonResponse(Ok(pretty(("id" -> id) ~ ("description" -> description))))
          case Some(badPath) =>
            badRequestResponse(s"file not found: $badPath")
          case None =>
            badRequestResponse(s"need 'uri' field with local file path as content")
        }
      }
    case GET -> Root / "reader" / id if logReaders.contains(id) =>
      implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      val logReader = logReaders(id).ref
      actorCall(logReader ? SparklintAppLogReader.GetReaderStatus, {
        case response: SparklintAppLogReader.GetReaderStatusResponse =>
          jsonResponse(Ok(pretty(renderGetReaderStatusResponse(response))))
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
          badRequestResponse(s"unrecognized command [$unrecognized]")
      }
      val pipeline = sendCommand.andThen(_ => {
        actorCall(logReader ? SparklintAppLogReader.GetReaderStatus, {
          case response: SparklintAppLogReader.GetReaderStatusResponse =>
            jsonResponse(Ok(pretty(renderGetReaderStatusResponse(response))))
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
    case req@GET -> Root / id / query if logReaders.contains(id) =>
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
        case "coreUsageByLocality" | "coreUsageByJobGroup" | "coreUsageByPool" =>
          val params = req.params
          Try(params("bucketSize").toLong).toOption match {
            case None =>
              badRequestResponse(s"bucketSize required")
            case Some(bucketSize) =>
              val from = Try(params("from").toLong).toOption
              val until = Try(params("until").toLong).toOption
              val message = query match {
                case "coreUsageByLocality" =>
                  JobSink.GetCoreUsageByLocality(bucketSize, from, until)
                case "coreUsageByJobGroup" =>
                  JobSink.GetCoreUsageByJobGroup(bucketSize, from, until)
                case "coreUsageByPool" =>
                  JobSink.GetCoreUsageByPool(bucketSize, from, until)
              }
              actorCall(logReader ? message, {
                case StandardMessages.NoDataYet =>
                  jsonResponse(Ok(pretty(("bucketSize" -> bucketSize) ~
                    ("numberOfBuckets" -> 0)
                  )))
                case JobSink.CoreUsageResponse(data) =>
                  jsonResponse(Ok(pretty(("bucketSize" -> bucketSize) ~
                    ("startedAt" -> data.firstKey) ~
                    ("startedTime" -> prettyPrintTime(data.firstKey)) ~
                    ("finishedAt" -> data.lastKey) ~
                    ("finishedTime" -> prettyPrintTime(data.lastKey)) ~
                    ("numberOfBuckets" -> (data.size - 1)) ~
                    ("buckets" -> data.map({ case (bucketStart, JobSink.UsageByGroup(byGroup, idle)) =>
                      ("bucketStartAt" -> bucketStart) ~
                        ("usageByGroup" -> byGroup.map({ case (groupName, usage) =>
                          ("name" -> groupName) ~ ("usage" -> usage)
                        })) ~
                        ("idle" -> idle)
                    }))
                  )))
              })
          }
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

  private def renderGetReaderStatusResponse(response: SparklintAppLogReader.GetReaderStatusResponse): JObject = {
    val storageOptionRepl = response.storageOption match {
      case StorageOption.Lossless =>
        JObject("storageType" -> JString("lossless"))
      case StorageOption.FixedCapacity(capacity, pruneFrequency) =>
        ("storageType" -> "fixed") ~
          ("capacity" -> capacity) ~
          ("pruneFrequency" -> pruneFrequency)
    }

    ("state" -> response.state) ~
      ("recordsRead" -> response.progress.numRead) ~
      ("lastRecord" -> response.lastRead.map(SparkEventToJson.apply)) ~
      ("storageOption" -> storageOptionRepl)

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
