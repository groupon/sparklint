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

import com.groupon.sparklint.analyzer.{SparklintAnalyzerLike, SparklintStateAnalyzer}
import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.events.{EventSourceManagerLike, EventSourceProgress}
import com.groupon.sparklint.server.{AdhocServer, StaticFileService}
import org.http4s.dsl._
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
    case GET -> Root                                                 => homepageHtml
    case GET -> Root / appId / "state" if appExists(appId)           => stateJson(appId)
    case GET -> Root / appId / "eventSource" if appExists(appId)     => eventSourceJson(appId)
    case GET -> Root / appId / "forward" / count if appExists(appId) => forwardAppJson(appId, count)
    case GET -> Root / appId / "rewind" / count if appExists(appId)  => rewindAppJson(appId, count)
    case GET -> Root / appId / "to_end" if appExists(appId)          => endAppJson(appId)
    case GET -> Root / appId / "to_start" if appExists(appId)        => startAppJson(appId)
  }

  private def appExists(appId: String) = esManager.containsAppId(appId)

  private def homepageHtml = {
    val page = new SparklintHomepage(esManager)
    htmlResponse(Ok(page.HTML.toString))
  }

  private def stateJson(appId: String) = {
    val es = esManager(appId)
    val report = SparklintStateAnalyzer(es.state)
    jsonResponse(Ok(pretty(UIServer.reportJson(report, es.progress))))
  }

  private def eventSourceJson(appId: String) = {
    jsonResponse(Ok(pretty(UIServer.progressJson(esManager(appId).progress))))
  }

  private def forwardAppJson(appId: String, count: String) = {
    moveEventSource(count, appId)(esManager.getCanFreeScrollEventSource(appId).forward)
  }

  private def rewindAppJson(appId: String, count: String) = {
    moveEventSource(count, appId)(esManager.getCanFreeScrollEventSource(appId).rewind)
  }

  private def endAppJson(appId: String) = {
    endOfEventSource(appId)(esManager.getCanFreeScrollEventSource(appId).end)
  }

  private def startAppJson(appId: String) = {
    endOfEventSource(appId)(esManager.getCanFreeScrollEventSource(appId).start)
  }

  private def moveEventSource(count: String, appId: String)(fx: (Int) => EventSourceProgress): Task[Response] = {
    Try(fx(count.toInt)) match {
      case Success(progress) =>
        jsonResponse(Ok(pretty(UIServer.progressJson(progress))))
      case Failure(ex)       =>
        logError(s"Failure to move appId $appId: ${ex.getMessage}")
        eventSourceJson(appId)
    }
  }

  private def endOfEventSource(appId: String)(fx: () => EventSourceProgress): Task[Response] = {
    Try(fx()) match {
      case Success(progress) =>
        jsonResponse(Ok(pretty(UIServer.progressJson(progress))))
      case Failure(ex)       =>
        logError(s"Failure to end of appId $appId: ${ex.getMessage}")
        eventSourceJson(appId)
    }
  }
}

object UIServer {
  def reportJson(report: SparklintStateAnalyzer, progress: EventSourceProgress): JObject = {
    implicit val formats = DefaultFormats
    ("appName" -> report.appName) ~
      ("appId" -> report.appId) ~
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
      ("applicationLaunchedAt" -> report.getApplicationLaunchedAt) ~
      ("applicationEndedAt" -> report.getApplicationEndedAt) ~
      ("progress" -> progressJson(progress))
  }

  def progressJson(progress: EventSourceProgress) = {
    ("percent" -> progress.percent) ~
      ("description" -> progress.description) ~
      ("at_start" -> progress.atStart) ~
      ("at_end" -> progress.atEnd) ~
      ("has_next" -> progress.hasNext) ~
      ("has_previous" -> progress.hasPrevious)
  }
}
