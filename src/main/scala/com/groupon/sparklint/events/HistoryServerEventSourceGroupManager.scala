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

package com.groupon.sparklint.events

import java.io.FileNotFoundException
import java.util.UUID

import org.http4s.MalformedMessageBodyFailure

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scalaz.{-\/, \/-}

/**
  * @author rxue
  * @since 1.0.5
  */
class HistoryServerEventSourceGroupManager(api: HistoryServerApi) extends GenericEventSourceGroupManager(api.name, true) {
  private val availableSourceMap: mutable.Map[String, EventSourceMeta] = mutable.Map.empty

  def availableSources: Seq[(String, EventSourceMeta)] = availableSourceMap.toSeq.sortBy(_._2.fullAppId).reverse

  def pull(): Unit = {
    availableSourceMap.clear()
    availableSourceMap ++= api.getApplications().flatMap(r => {
      r.attempts.map(attempt => {
        UUID.randomUUID().toString -> EventSourceMeta(Some(r.id), attempt.attemptId, r.name, None, attempt.startTime.getTime)
      })
    })
  }

  def pullEventSource(esUuid: String): Try[FreeScrollEventSource] = {
    if (availableSourceMap.contains(esUuid)) {
      val meta = availableSourceMap.remove(esUuid).get
      api.getLogs(esUuid, meta).attemptRun match {
        case \/-(es) =>
          registerEventSource(es)
          Success(es)
        case -\/(ex: MalformedMessageBodyFailure) =>
          Failure(ex)
        case -\/(ex) =>
          availableSourceMap(esUuid) = meta
          Failure(ex)
      }
    } else {
      Failure(new FileNotFoundException(s"EventSource $esUuid doesn't exist in HistoryServerEventSourceGroupManager $name"))

    }
  }
}
