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

import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.event.{EventSourceGroupManager, FolderEventSourceGroupManager, HistoryServerApi, HistoryServerEventSourceGroupManager}
import com.groupon.sparklint.events._
import org.http4s.MediaType.`application/json`
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.{HttpService, Response, Uri}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, pretty}
import org.json4s.{DefaultFormats, Extraction}

import scala.collection.mutable
import scala.util.{Failure, Success}
import scalaz.concurrent.Task

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
      jsonResponse(Ok(state(esmId, esId)))
    case GET -> Root / "esm" / esmId / esId / "eventSource" if appExists(esmId, esId) =>
      jsonResponse(Ok(eventSource(esmId, esId)))
    case GET -> Root / "esm" / esmId / esId / "forward" / count / evString if appExists(esmId, esId) =>
      jsonResponse(Ok(fwdApp(esmId, esId, count, evString)))
    case GET -> Root / "esm" / esmId / esId / "rewind" / count / evString if appExists(esmId, esId) =>
      jsonResponse(Ok(rwdApp(esmId, esId, count, evString)))
    case GET -> Root / "esm" / esmId / esId / "to_end" if appExists(esmId, esId) =>
      jsonResponse(Ok(endApp(esmId, esId)))
    case GET -> Root / "esm" / esmId / esId / "to_start" if appExists(esmId, esId) =>
      jsonResponse(Ok(startApp(esmId, esId)))
    case POST -> Root / "esm" / esmId / esId / "activate" =>
      activateApp(esmId, esId)
  }

  private def appExists(esmId: String, esId: String): Boolean = {
    esgManagers.get(esmId).exists(_.containsEventSources(esId))
  }

  private def state(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getEventSources(esUuid)
    val report = new SparklintStateAnalyzer(eventSource.appMeta, eventSource.appState)
    pretty(SparklintBackend.reportJson(report, eventSource.progressTracker))
  }

  private def eventSource(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getEventSources(esUuid)
    pretty(SparklintBackend.progressJson(eventSource.progressTracker))
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
    pretty(SparklintBackend.progressJson(eventSource.progressTracker))
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
    pretty(SparklintBackend.progressJson(eventSource.progressTracker))
  }

  private def endApp(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getFreeScrollEventSource(esUuid)
    eventSource.toEnd()
    pretty(SparklintBackend.progressJson(eventSource.progressTracker))
  }

  private def startApp(esgmUuid: String, esUuid: String): String = {
    val eventSource = esgManagers(esgmUuid).getFreeScrollEventSource(esUuid)
    eventSource.toStart()
    pretty(SparklintBackend.progressJson(eventSource.progressTracker))
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
        jsonResponse(Ok(compact("message" -> es.appMeta.toString)))
      case Failure(ex) =>
        logError(s"Failure to activate.", ex)
        jsonResponse(InternalServerError(compact("message" -> ex.getMessage)))
    }
  }

  private def jsonResponse(textResponse: Task[Response]): Task[Response] = {
    textResponse.withContentType(Some(`Content-Type`(`application/json`)))
  }

  protected def appendFolderManager(folder: File): Unit = {
    append(new FolderEventSourceGroupManager(folder))
  }

  /** Appends the given [[EventSourceGroupManager]]
    *
    * @param esgm the [[EventSourceGroupManager]] to append.
    */
  def append(esgm: EventSourceGroupManager): Unit = {
    esgManagers(esgm.uuid.toString) = esgm
  }

  protected def appendHistoryServer(serverName: String, historyServerHost: String): Unit = {
    val api = HistoryServerApi(serverName, Uri.fromString(historyServerHost).toOption.get)
    append(new HistoryServerEventSourceGroupManager(api))
  }

}

object SparklintBackend {
  def reportJson(report: SparklintStateAnalyzer,
                 progress: EventProgressTrackerLike): JObject = {
    implicit val formats = DefaultFormats
    val source = report.source
    ("appName" -> source.appName) ~
      ("appId" -> source.appId) ~
      ("appAttemptId" -> source.attempt) ~
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
          ("any" -> tuple.any) ~
          ("processLocal" -> tuple.processLocal) ~
          ("nodeLocal" -> tuple.nodeLocal) ~
          ("rackLocal" -> tuple.rackLocal) ~
          ("noPref" -> tuple.noPref)
      }))) ~
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
      ("applicationLaunchedAt" -> source.startTime) ~
      ("applicationEndedAt" -> source.endTime) ~
      ("progress" -> progressJson(progress))
  }

  def progressJson(progressTracker: EventProgressTrackerLike): JObject = {
    ("percent" -> progressTracker.eventProgress.percent) ~
      ("description" -> progressTracker.eventProgress.description) ~
      ("has_next" -> progressTracker.eventProgress.hasNext) ~
      ("has_previous" -> progressTracker.eventProgress.hasPrevious)
  }
}
