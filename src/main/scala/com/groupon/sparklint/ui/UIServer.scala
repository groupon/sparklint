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
import com.groupon.sparklint.events.{EventSourceLike, EventSourceManagerLike, EventSourceProgress}
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
    case GET -> Root                                                          => homepageHtml
    case GET -> Root / appId / "state" if appExists(appId)                    => progressJson(appId)
    case GET -> Root / appId / "eventSource" if appExists(appId)              => eventSourceJson(appId)
    case GET -> Root / appId / "forward" / count / evType if appExists(appId) => forwardAppJson(appId, count, evType)
    case GET -> Root / appId / "rewind" / count / evType if appExists(appId)  => rewindAppJson(appId, count, evType)
    case GET -> Root / appId / "to_end" if appExists(appId)                   => endAppJson(appId)
    case GET -> Root / appId / "to_start" if appExists(appId)                 => startAppJson(appId)
  }

  private def appExists(appId: String) = {
    esManager.containsAppId(appId)
  }

  private def homepageHtml = {
    val page = new SparklintHomepage(esManager)
    htmlResponse(Ok(page.HTML.toString))
  }

  private def progressJson(appId: String) = {
    val es = esManager.getSource(appId)
    val report = SparklintStateAnalyzer(es)
    jsonResponse(Ok(pretty(UIServer.reportJson(report, es))))
  }

  private def eventSourceJson(appId: String) = {
    jsonResponse(Ok(pretty(UIServer.progressJson(esManager.getSource(appId).progress))))
  }

  private def forwardAppJson(appId: String, count: String, evType: String) = evType.toLowerCase match {
    case `eventsNav` => moveEventSource(count, appId)(esManager.getScrollingSource(appId).forwardEvents)
    case `tasksNav`  => moveEventSource(count, appId)(esManager.getScrollingSource(appId).forwardTasks)
    case `stagesNav` => moveEventSource(count, appId)(esManager.getScrollingSource(appId).forwardStages)
    case `jobsNav`   => moveEventSource(count, appId)(esManager.getScrollingSource(appId).forwardJobs)
  }

  private def rewindAppJson(appId: String, count: String, evType: String) = evType.toLowerCase match {
    case `eventsNav` => moveEventSource(count, appId)(esManager.getScrollingSource(appId).rewindEvents)
    case `tasksNav`  => moveEventSource(count, appId)(esManager.getScrollingSource(appId).rewindTasks)
    case `stagesNav` => moveEventSource(count, appId)(esManager.getScrollingSource(appId).rewindStages)
    case `jobsNav`   => moveEventSource(count, appId)(esManager.getScrollingSource(appId).rewindJobs)
  }

  private def endAppJson(appId: String) = {
    endOfEventSource(appId)(esManager.getScrollingSource(appId).toEnd)
  }

  private def startAppJson(appId: String) = {
    endOfEventSource(appId)(esManager.getScrollingSource(appId).toStart)
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

  private val eventsNav = UIServer.NAV_TYPE_EVENTS.toLowerCase
  private val tasksNav  = UIServer.NAV_TYPE_TASKS.toLowerCase
  private val stagesNav = UIServer.NAV_TYPE_STAGES.toLowerCase
  private val jobsNav   = UIServer.NAV_TYPE_JOBS.toLowerCase
}

object UIServer {

  val NAV_TYPE_EVENTS = "Events"
  val NAV_TYPE_TASKS  = "Tasks"
  val NAV_TYPE_STAGES = "Stages"
  val NAV_TYPE_JOBS   = "Jobs"

  def supportedNavTypes: Seq[String] = Seq(NAV_TYPE_EVENTS, NAV_TYPE_TASKS, NAV_TYPE_STAGES, NAV_TYPE_JOBS)

  def reportJson(report: SparklintStateAnalyzer, es: EventSourceLike): JObject = {
    implicit val formats = DefaultFormats

    val progress = es.progress

    ("appName" -> es.appName) ~
      ("appId" -> es.appId) ~
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
      ("applicationLaunchedAt" -> es.startTime) ~
      ("applicationEndedAt" -> es.endTime) ~
      ("progress" -> progressJson(progress))
  }

  def progressJson(progress: EventSourceProgress) = {
    ("percent" -> progress.percent) ~
      ("description" -> progress.description) ~
      ("has_next" -> progress.hasNext) ~
      ("has_previous" -> progress.hasPrevious)
  }
}
