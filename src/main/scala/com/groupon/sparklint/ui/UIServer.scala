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

import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.events._
import com.groupon.sparklint.server.{AdhocServer, StaticFileService}
import org.http4s.{HttpService, Response}
import org.http4s.dsl._
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
class UIServer(esManager: EventSourceManagerLike)
  extends AdhocServer with StaticFileService with Logging {

  routingMap("/") = sparklintService

  private def sparklintService = HttpService {
    case GET -> Root                                                            => tryit(homepage, htmlResponse)
    case GET -> Root / appId / "state" if appExists(appId)                      => tryit(state(appId))
    case GET -> Root / appId / "eventSource" if appExists(appId)                => tryit(eventSource(appId))
    case GET -> Root / appId / "forward" / count / evString if appExists(appId) => tryit(fwdApp(appId, count, evString))
    case GET -> Root / appId / "rewind" / count / evString if appExists(appId)  => tryit(rwdApp(appId, count, evString))
    case GET -> Root / appId / "to_end" if appExists(appId)                     => tryit(endApp(appId))
    case GET -> Root / appId / "to_start" if appExists(appId)                   => tryit(startApp(appId))
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

  private def appExists(appId: String): Boolean = {
    esManager.containsEventSourceId(appId)
  }

  private def homepage: String = {
    val page = new SparklintHomepage(esManager)
    page.HTML.toString
  }

  private def state(appId: String): String = {
    val detail = esManager.getSourceDetail(appId)
    val report = new SparklintStateAnalyzer(detail.meta, detail.state)
    pretty(UIServer.reportJson(report, detail.progress))
  }

  private def eventSource(appId: String): String = {
    pretty(UIServer.progressJson(esManager.getSourceDetail(appId).progress))
  }

  private def fwdApp(appId: String, count: String, evString: String): String = {
    fwdApp(appId, count, EventType.fromString(evString))
  }

  private def fwdApp(appId: String, count: String, evType: EventType): String = {
    def progress() = esManager.getSourceDetail(appId).progress
    val mover = moveEventSource(count, appId, progress) _
    val eventSource = esManager.getScrollingSource(appId)
    evType match {
      case Events => mover(eventSource.forwardEvents)
      case Tasks  => mover(eventSource.forwardTasks)
      case Stages => mover(eventSource.forwardStages)
      case Jobs   => mover(eventSource.forwardJobs)
    }
  }

  private def rwdApp(appId: String, count: String, evString: String): String = {
    rwdApp(appId, count, EventType.fromString(evString))
  }

  private def rwdApp(appId: String, count: String, evType: EventType): String = {
    def progress() = esManager.getSourceDetail(appId).progress
    val mover = moveEventSource(count, appId, progress) _
    val eventSource = esManager.getScrollingSource(appId)
    evType match {
      case Events => mover(eventSource.rewindEvents)
      case Tasks  => mover(eventSource.rewindTasks)
      case Stages => mover(eventSource.rewindStages)
      case Jobs   => mover(eventSource.rewindJobs)
    }
  }

  private def endApp(appId: String): String = {
    endOfEventSource(appId,
      () => esManager.getScrollingSource(appId).toEnd(),
      () => esManager.getSourceDetail(appId).progress)
  }

  private def startApp(appId: String): String = {
    endOfEventSource(appId,
      () => esManager.getScrollingSource(appId).toStart(),
      () => esManager.getSourceDetail(appId).progress)
  }

  private def moveEventSource(count: String, appId: String, progFn: () => EventProgressTrackerLike)
                             (moveFn: (Int) => Unit): String = {
    Try(moveFn(count.toInt)) match {
      case Success(progress) =>
        pretty(UIServer.progressJson(progFn()))
      case Failure(ex)       =>
        logError(s"Failure to move appId $appId: ${ex.getMessage}")
        eventSource(appId)
    }
  }

  private def endOfEventSource(appId: String,
                               moveFn: () => Unit,
                               progFn: () => EventProgressTrackerLike): String = {
    Try(moveFn()) match {
      case Success(unit) =>
        pretty(UIServer.progressJson(progFn()))
      case Failure(ex)   =>
        logError(s"Failure to end of appId $appId: ${ex.getMessage}")
        eventSource(appId)
    }
  }
}

object UIServer {

  def reportJson(report: SparklintStateAnalyzer,
                 progress: EventProgressTrackerLike): JObject = {
    implicit val formats = DefaultFormats
    val source = report.source
    ("appName" -> source.appName) ~
      ("appId" -> source.appId) ~
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
