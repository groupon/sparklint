/*
 Copyright 2016 Groupon, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.groupon.sparklint.ui

import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.events._
import com.groupon.sparklint.server.{AdhocServer, StaticFileService}
import org.http4s.dsl.{->, /, _}
import org.http4s.{HttpService, Response}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Extraction}

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
    case GET -> Root                                                            => homepageHtml
    case GET -> Root / appId / "state" if appExists(appId)                      => progressJson(appId)
    case GET -> Root / appId / "eventSource" if appExists(appId)                => eventSourceJson(appId)
    case GET -> Root / appId / "forward" / count / evString if appExists(appId) => fwdAppJson(appId, count, evString)
    case GET -> Root / appId / "rewind" / count / evString if appExists(appId)  => rwdAppJson(appId, count, evString)
    case GET -> Root / appId / "to_end" if appExists(appId)                     => endAppJson(appId)
    case GET -> Root / appId / "to_start" if appExists(appId)                   => startAppJson(appId)
  }

  private def appExists(appId: String) = {
    esManager.containsAppId(appId)
  }

  private def homepageHtml = {
    val page = new SparklintHomepage(esManager)
    htmlResponse(Ok(page.HTML.toString))
  }

  private def progressJson(appId: String) = {
    val detail = esManager.getSourceDetail(appId)
    val report = SparklintStateAnalyzer(detail.source, detail.state)
    jsonResponse(Ok(pretty(UIServer.reportJson(report, detail.source, detail.progress))))
  }

  private def eventSourceJson(appId: String) = {
    jsonResponse(Ok(pretty(UIServer.progressJson(esManager.getSourceDetail(appId).progress))))
  }

  private def fwdAppJson(appId: String, count: String, evString: String): Task[Response] = {
    fwdAppJson(appId, count, EventType.fromString(evString))

  }

  private def fwdAppJson(appId: String, count: String, evType: EventType): Task[Response] = {
    def progress() = esManager.getSourceDetail(appId).progress
    val mover = moveEventSource(count, appId, progress) _
    evType match {
      case Events() => mover(esManager.getScrollingSource(appId).forwardEvents)
      case Tasks()  => mover(esManager.getScrollingSource(appId).forwardTasks)
      case Stages() => mover(esManager.getScrollingSource(appId).forwardStages)
      case Jobs()   => mover(esManager.getScrollingSource(appId).forwardJobs)
    }
  }

  private def rwdAppJson(appId: String, count: String, evString: String): Task[Response] = {
    rwdAppJson(appId, count, EventType.fromString(evString))

  }

  private def rwdAppJson(appId: String, count: String, evType: EventType): Task[Response] = {
    def progress() = esManager.getSourceDetail(appId).progress
    val mover = moveEventSource(count, appId, progress) _
    evType match {
      case Events() => mover(esManager.getScrollingSource(appId).rewindEvents)
      case Tasks()  => mover(esManager.getScrollingSource(appId).rewindTasks)
      case Stages() => mover(esManager.getScrollingSource(appId).rewindStages)
      case Jobs()   => mover(esManager.getScrollingSource(appId).rewindJobs)
    }
  }

  private def endAppJson(appId: String) = {
    endOfEventSource(appId,
      (appid) => esManager.getScrollingSource(appid).toEnd(),
      (appid) => esManager.getSourceDetail(appid).progress)
  }

  private def startAppJson(appId: String) = {
    endOfEventSource(appId,
      (appid) => esManager.getScrollingSource(appId).toStart(),
      (appid) => esManager.getSourceDetail(appid).progress)
  }

  private def moveEventSource(count: String, appId: String, progFn: () => EventSourceProgressLike)
                             (moveFn: (Int) => Unit): Task[Response] = {
    Try(moveFn(count.toInt)) match {
      case Success(progress) =>
        jsonResponse(Ok(pretty(UIServer.progressJson(progFn()))))
      case Failure(ex)       =>
        logError(s"Failure to move appId $appId: ${ex.getMessage}")
        eventSourceJson(appId)
    }
  }

  private def endOfEventSource(appId: String,
                               moveFn: (String) => Unit,
                               progFn: (String) => EventSourceProgressLike): Task[Response] = {
    Try(moveFn(appId)) match {
      case Success(unit) =>
        jsonResponse(Ok(pretty(UIServer.progressJson(progFn(appId)))))
      case Failure(ex)   =>
        logError(s"Failure to end of appId $appId: ${ex.getMessage}")
        eventSourceJson(appId)
    }
  }
}

object UIServer {

  def reportJson(report: SparklintStateAnalyzer,
                 source: EventSourceLike,
                 progress: EventSourceProgressLike): JObject = {
    implicit val formats = DefaultFormats

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

  def progressJson(progress: EventSourceProgressLike) = {
    ("percent" -> progress.eventProgress.percent) ~
      ("description" -> progress.eventProgress.description) ~
      ("has_next" -> progress.eventProgress.hasNext) ~
      ("has_previous" -> progress.eventProgress.hasPrevious)
  }
}
