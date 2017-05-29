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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.events._
import org.http4s.MediaType.`application/json`
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.server.websocket.WS
import org.http4s.websocket.WebsocketBits.{Close, Text, WebSocketFrame}
import org.http4s.{HttpService, Response, Uri}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, pretty}
import org.json4s.{DefaultFormats, Extraction}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Strategy.DefaultStrategy
import scalaz.concurrent.Task
import scalaz.stream.async.unboundedQueue
import scalaz.stream.time.awakeEvery
import scalaz.stream.{DefaultScheduler, Exchange}
import scalaz.{-\/, \/-}

/**
  * The backend api
  *
  * @author rxue
  * @since 1.0.5
  */
class SparklintBackend
  extends Logging {
  private val esgManagers = mutable.Map[String, EventSourceGroupManager]()

  /**
    * @return All available [[EventSourceGroupManager]]
    */
  def listEventSourceGroupManagers: Seq[EventSourceGroupManager] = esgManagers.values.toList

  def backendService: HttpService = HttpService {
    case GET -> Root / "esm" / esmId / esId / "state" if appExists(esmId, esId) =>
      tryit(Try(state(esmId, esId)), jsonResponse)
    case GET -> Root / "esm" / esmId / esId / "stateWS" if appExists(esmId, esId) =>
      val statusStream = awakeEvery(3 seconds)(DefaultStrategy, DefaultScheduler).map { (timeSinceStart) =>
        Text(state(esmId, esId))
      }
      val q = unboundedQueue[WebSocketFrame]
      WS(Exchange(statusStream, q.enqueue))
    case GET -> Root / "esm" / esmId / esId / "eventSource" if appExists(esmId, esId) =>
      tryit(Try(eventSource(esmId, esId)), jsonResponse)
    case GET -> Root / "esm" / esmId / esId / "forward" / count / evString if appExists(esmId, esId) =>
      tryit(Try(fwdApp(esmId, esId, count, evString)), jsonResponse)
    case GET -> Root / "esm" / esmId / esId / "rewind" / count / evString if appExists(esmId, esId) =>
      tryit(Try(rwdApp(esmId, esId, count, evString)), jsonResponse)
    case GET -> Root / "esm" / esmId / esId / "to_end" if appExists(esmId, esId) =>
      tryit(Try(endApp(esmId, esId)), jsonResponse)
    case GET -> Root / "esm" / esmId / esId / "to_start" if appExists(esmId, esId) =>
      tryit(Try(startApp(esmId, esId)), jsonResponse)
    case POST -> Root / "esm" / esmId / esId / "activate" =>
      activateApp(esmId, esId)
    case req@POST -> Root / "esm" / "singleFile" =>
      req.decode[String] { fileName =>
        tryit(Try(tryAppendSingleFileManager(fileName)), jsonResponse)
      }
    case req@POST -> Root / "esm" / "folder" =>
      req.decode[String] { folderName =>
        tryit(Try(tryAppendFolderManager(folderName)), jsonResponse)
      }
    case req@POST -> Root / "esm" / "historyServer" =>
      req.decode[String] { serverHost =>
        tryit(Try(tryAppendHistoryServer(serverHost)), jsonResponse)
      }
  }

  private def tryit(exec: Try[String],
                    formatFx: (Task[Response]) => Task[Response] = jsonResponse): Task[Response] = {
    exec match {
      case Success(result) =>
        formatFx(Ok(result))
      case Failure(ex) =>
        logError(s"Failure to navigate.", ex)
        InternalServerError(ex.getMessage)
    }
  }

  private def appExists(esmId: String, esUuid: String): Boolean = {
    esgManagers.get(esmId).exists(_.containsEventSources(esUuid))
  }

  private def state(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getEventSources(esUuid)
    val report = new SparklintStateAnalyzer(eventSource.appMeta, eventSource.appState)
    pretty(SparklintBackend.reportJson(eventSource, report))
  }

  private def eventSource(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getEventSources(esUuid)
    pretty(SparklintBackend.progressJson(eventSource))
  }

  private def fwdApp(esgmUuid: String, esUuid: String, count: String, evString: String): String = {
    fwdApp(esgmUuid, esUuid, count.toInt, EventType.fromString(evString))
  }

  private def fwdApp(esgmUuid: String, esUuid: String, count: Int, evType: EventType): String = {
    val eventSource = esgManagers(esgmUuid).getFreeScrollEventSource(esUuid)
    evType match {
      case Events => eventSource.forwardEvents(count)
      case Tasks => eventSource.forwardTasks(count)
      case Stages => eventSource.forwardStages(count)
      case Jobs => eventSource.forwardJobs(count)
    }
    pretty(SparklintBackend.progressJson(eventSource))
  }

  private def rwdApp(esgmUuid: String, esUuid: String, count: String, evString: String): String = {
    rwdApp(esgmUuid, esUuid, count.toInt, EventType.fromString(evString))
  }

  private def rwdApp(esgmUuid: String, esUuid: String, count: Int, evType: EventType): String = {
    val eventSource = esgManagers(esgmUuid).getFreeScrollEventSource(esUuid)
    evType match {
      case Events => eventSource.rewindEvents(count)
      case Tasks => eventSource.rewindTasks(count)
      case Stages => eventSource.rewindStages(count)
      case Jobs => eventSource.rewindJobs(count)
    }
    pretty(SparklintBackend.progressJson(eventSource))
  }

  private def endApp(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getFreeScrollEventSource(esUuid)
    eventSource.toEnd()
    pretty(SparklintBackend.progressJson(eventSource))
  }

  private def startApp(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getFreeScrollEventSource(esUuid)
    eventSource.toStart()
    pretty(SparklintBackend.progressJson(eventSource))
  }

  private def activateApp(esgmUuid: String, appId: String): Task[Response] = {
    val esgm = esgManagers(esgmUuid)
    val t = esgm match {
      case history: HistoryServerEventSourceGroupManager =>
        history.pullEventSource(appId)
      case _ =>
        Failure(new NoSuchElementException())
    }
    t match {
      case Success(es) =>
        Future(es.toEnd())
        jsonResponse(Ok(compact("message" -> es.appMeta.toString)))
      case Failure(ex) =>
        logError(s"Failure to activate.", ex)
        jsonResponse(InternalServerError(compact("message" -> ex.getMessage)))
    }
  }

  private def jsonResponse(textResponse: Task[Response]): Task[Response] = {
    textResponse.withContentType(Some(`Content-Type`(`application/json`)))
  }

  private def tryAppendSingleFileManager(fileName: String): String = {
    val esgm = appendSingleFileManager(new File(fileName))
    pretty(("message" -> s"successfully added eventSourceManager ${esgm.name}") ~
      ("uuid" -> esgm.uuid.toString))
  }

  def appendSingleFileManager(file: File): EventSourceGroupManager = {
    val manager = new GenericEventSourceGroupManager(file.getName, true)
    val es = EventSource.fromFile(file)
    Future(es.toEnd())
    manager.registerEventSource(es)
    append(manager)
  }

  /** Appends the given [[EventSourceGroupManager]]
    *
    * @param esgm the [[EventSourceGroupManager]] to append.
    */
  def append(esgm: EventSourceGroupManager): EventSourceGroupManager = {
    esgManagers(esgm.uuid.toString) = esgm
    esgm
  }

  private def tryAppendFolderManager(folderName: String): String = {
    val esgm = appendFolderManager(new File(folderName))
    pretty(("message" -> s"successfully added eventSourceManager ${esgm.name}") ~
      ("uuid" -> esgm.uuid.toString))
  }

  def appendFolderManager(folder: File): EventSourceGroupManager = {
    val manager = new FolderEventSourceGroupManager(folder)
    manager.pull()
    append(manager)
  }

  private def tryAppendHistoryServer(historyServerHost: String): String = {
    Uri.fromString(historyServerHost) match {
      case -\/(ex) => throw ex
      case \/-(uri) =>
        val esgm = appendHistoryServer(uri.host.get.value, uri)
        pretty(("message" -> s"successfully added eventSourceManager ${esgm.name}") ~
          ("uuid" -> esgm.uuid.toString))

    }
  }

  def appendHistoryServer(serverName: String, historyServerHost: Uri): EventSourceGroupManager = {
    val api = HistoryServerApi(serverName, historyServerHost)
    val manager = new HistoryServerEventSourceGroupManager(api)
    manager.pull()
    append(manager)
  }

}

object SparklintBackend {
  def reportJson(source: EventSource, report: SparklintStateAnalyzer): JObject = {
    implicit val formats = DefaultFormats
    val meta = report.meta
    ("appName" -> meta.appName) ~
      ("appId" -> meta.appId) ~
      ("appAttemptId" -> meta.attempt) ~
      ("allocatedCores" -> report.getExecutorInfo.map(_.values.map(_.cores).sum)) ~
      ("executors" -> report.getExecutorInfo.map(_.map({ case (executorId, info) =>
        ("executorId" -> executorId) ~
          ("cores" -> info.cores) ~
          ("start" -> info.startTime) ~
          ("end" -> info.endTime)
      }))) ~
      ("currentCores" -> report.getCurrentCores) ~
      ("runningTasks" -> report.getRunningTasks) ~
      ("currentTaskByExecutor" -> report.getCurrentTaskByExecutors.map(_.map({ case (executorId, tasks) =>
        ("executorId" -> executorId) ~
          ("tasks" -> tasks.map(task => Extraction.decompose(task)))
      }))) ~
      ("timeUntilFirstTask" -> report.getTimeUntilFirstTask) ~
      ("timeSeriesCoreUsage" -> report.getTimeSeriesCoreUsage.map(_.sortBy(_.time).map(tuple => {
        ("time" -> tuple.time) ~
          ("idle" -> tuple.idle) ~
          tuple.jObjectByLocality ~
          tuple.jObjectByPool
      }))) ~
      ("pools" -> report.getFairSchedulerPools) ~
      ("cumulativeCoreUsage" -> report.getCumulativeCoreUsage.map(_.toSeq.sortBy(_._1).map { case (core, duration) =>
        ("cores" -> core) ~
          ("duration" -> duration)
      })) ~
      ("idleTime" -> report.getIdleTime) ~
      ("idleTimeSinceFirstTask" -> report.getIdleTimeSinceFirstTask) ~
      ("maxConcurrentTasks" -> report.getMaxConcurrentTasks) ~
      ("maxAllocatedCores" -> report.getMaxAllocatedCores) ~
      ("maxCoreUsage" -> report.getMaxCoreUsage) ~
      ("coreUtilizationPercentage" -> report.getCoreUtilizationPercentage) ~
      ("lastUpdatedAt" -> report.getLastUpdatedAt) ~
      ("applicationLaunchedAt" -> meta.startTime) ~
      ("applicationEndedAt" -> meta.endTime) ~
      ("progress" -> progressJson(source))
  }

  def progressJson(eventSource: EventSource): JObject = {
    ("percent" -> eventSource.progressTracker.eventProgress.percent) ~
      ("description" -> eventSource.progressTracker.eventProgress.description) ~
      ("has_next" -> eventSource.hasNext) ~
      ("has_previous" -> eventSource.hasPrevious)
  }
}
