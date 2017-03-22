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

package com.groupon.sparklint.ui

import java.util.NoSuchElementException

import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import com.groupon.sparklint.common.{Logging, SparklintConfig}
import com.groupon.sparklint.events.{RootEventSourceManager, _}
import com.groupon.sparklint.server.{AdhocServer, StaticFileService}
import com.groupon.sparklint.ui.views.{SparklintHomepage, UIEventSourceNavigation}
import org.http4s.dsl._
import org.http4s.{HttpService, Response}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Extraction, JObject}

import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Task

/**
  *
  * @author swhitear
  * @since 8/18/16.
  */
class UIServer(esManager: RootEventSourceManager, config: SparklintConfig)
  extends AdhocServer with StaticFileService with Logging {

  routingMap("/") = sparklintService

  override def DEFAULT_PORT: Int = config.port

  private def sparklintService = HttpService {
    case GET -> Root                                                                                   =>
      tryit(homepage, htmlResponse)
    case GET -> Root / "eventSourceManagerList"                                                        =>
      tryit(eventSourceManagerList, htmlResponse)
    case GET -> Root / "esm" / esmId / appId / "state" if appExists(esmId, appId)                      =>
      tryit(state(esmId, appId))
    case GET -> Root / "esm" / esmId / appId / "eventSource" if appExists(esmId, appId)                =>
      tryit(eventSource(esmId, appId))
    case GET -> Root / "esm" / esmId / appId / "forward" / count / evString if appExists(esmId, appId) =>
      tryit(fwdApp(esmId, appId, count, evString))
    case GET -> Root / "esm" / esmId / appId / "rewind" / count / evString if appExists(esmId, appId)  =>
      tryit(rwdApp(esmId, appId, count, evString))
    case GET -> Root / "esm" / esmId / appId / "to_end" if appExists(esmId, appId)                     =>
      tryit(endApp(esmId, appId))
    case GET -> Root / "esm" / esmId / appId / "to_start" if appExists(esmId, appId)                   =>
      tryit(startApp(esmId, appId))
    case POST -> Root / "esm" / esmId / appId / "activate"                                             =>
      activateApp(esmId, appId)
  }

  private def tryit(exec: => String,
                    formatFx: (Task[Response]) => Task[Response] = jsonResponse): Task[Response] = {
    Try(exec) match {
      case Success(result) =>
        formatFx(Ok(result))
      case Failure(ex)     =>
        logError(s"Failure to navigate.", ex)
        htmlResponse(InternalServerError(ex.getMessage))
    }
  }

  private def appExists(esmUuid: String, appId: String): Boolean = {
    esManager.eventSourceManager(esmUuid).exists(_.containsEventSource(appId))
  }

  private def homepage: String = {
    val page = new SparklintHomepage
    page.HTML.toString
  }

  private def eventSourceManagerList: String = {
    UIEventSourceNavigation(esManager).mkString("\n")
  }

  private def state(esmUuid: String, appId: String): String = {
    val detail = esManager.eventSourceManager(esmUuid).get.getSourceDetail(appId)
    val report = new SparklintStateAnalyzer(detail.meta, detail.state)
    pretty(UIServer.reportJson(report, detail.progress))
  }

  private def eventSource(esmUuid: String, appId: String): String = {
    pretty(UIServer.progressJson(esManager.eventSourceManager(esmUuid).get.getSourceDetail(appId).progress))
  }

  private def fwdApp(esmUuid: String, appId: String, count: String, evString: String): String = {
    fwdApp(esmUuid, appId, count, EventType.fromString(evString))
  }

  private def fwdApp(esmUuid: String, appId: String, count: String, evType: EventType): String = {
    val esm = esManager.eventSourceManager(esmUuid).get
    def progress() = esm.getSourceDetail(appId).progress

    val mover = moveEventSource(count, progress) _
    val eventSource = esm.getScrollingSource(appId)
    evType match {
      case Events => mover(eventSource.forwardEvents)
      case Tasks  => mover(eventSource.forwardTasks)
      case Stages => mover(eventSource.forwardStages)
      case Jobs   => mover(eventSource.forwardJobs)
    }
  }

  private def rwdApp(esmUuid: String, appId: String, count: String, evString: String): String = {
    rwdApp(esmUuid, appId, count, EventType.fromString(evString))
  }

  private def rwdApp(esmUuid: String, appId: String, count: String, evType: EventType): String = {
    val esm = esManager.eventSourceManager(esmUuid).get
    def progress() = esm.getSourceDetail(appId).progress

    val mover = moveEventSource(count, progress) _
    val eventSource = esm.getScrollingSource(appId)
    evType match {
      case Events => mover(eventSource.rewindEvents)
      case Tasks  => mover(eventSource.rewindTasks)
      case Stages => mover(eventSource.rewindStages)
      case Jobs   => mover(eventSource.rewindJobs)
    }
  }

  private def endApp(esmUuid: String, appId: String): String = {
    val esm = esManager.eventSourceManager(esmUuid).get
    endOfEventSource(() => esm.getScrollingSource(appId).toEnd(),
      () => esm.getSourceDetail(appId).progress)
  }

  private def startApp(esmUuid: String, appId: String): String = {
    val esm = esManager.eventSourceManager(esmUuid).get
    endOfEventSource(() => esm.getScrollingSource(appId).toStart(),
      () => esm.getSourceDetail(appId).progress)
  }

  private def moveEventSource(count: String, progFn: () => EventProgressTrackerLike)
                             (moveFn: (Int) => Unit): String = {
    moveFn(count.toInt)
    pretty(UIServer.progressJson(progFn()))
  }

  private def endOfEventSource(moveFn: () => Unit,
                               progFn: () => EventProgressTrackerLike): String = {
    moveFn()
    pretty(UIServer.progressJson(progFn()))
  }

  private def activateApp(esmUuid: String, appId: String): Task[Response] = {
    val esm = esManager.eventSourceManager(esmUuid).get
    val t = esm match {
      case desm: DirectoryEventSourceManager =>
        desm.pullEventSource(appId)
      case hsesm: HistoryServerEventSourceManager =>
        hsesm.pullEventSource(appId)
      case _ =>
        Failure(new NoSuchElementException())
    }
    t match {
      case Success(_) =>
        jsonResponse(Ok(compact("status" -> "ok")))
      case Failure(ex)     =>
        logError(s"Failure to navigate.", ex)
        htmlResponse(InternalServerError(ex.getMessage))
    }
  }
}

object UIServer {

  def reportJson(report: SparklintStateAnalyzer,
                 progress: EventProgressTrackerLike): JObject = {
    implicit val formats = DefaultFormats
    val source = report.source
    ("appName" -> source.appName) ~
      ("appId" -> source.appIdentifier.appId) ~
      ("appAttemptId" -> source.appIdentifier.attemptId) ~
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

  def progressJson(progressTracker: EventProgressTrackerLike) = {
    ("percent" -> progressTracker.eventProgress.percent) ~
      ("description" -> progressTracker.eventProgress.description) ~
      ("has_next" -> progressTracker.eventProgress.hasNext) ~
      ("has_previous" -> progressTracker.eventProgress.hasPrevious)
  }
}
