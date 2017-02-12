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


import scala.collection.mutable
import scala.util.Try

/**
  * @author rxue
  * @since 2/5/17.
  */
class HistoryServerEventSourceManager(historyServer: HistoryServerApi) extends EventSourceManagerLike {
  private val eventSources: mutable.Map[EventSourceIdentifier, EventSourceLike] = new mutable.LinkedHashMap[EventSourceIdentifier, EventSourceLike] with mutable.SynchronizedMap[EventSourceIdentifier, EventSourceLike]

  // TODO: pull available logs from history api
  def pull(): Unit = ???

  def pullEventSource(id: EventSourceIdentifier): Try[Unit] = Try {
    // TODO: download event logs
  }

  override def sourceCount: Int = eventSources.size

  override def displayName: String = historyServer.name

  override def displayDetails: String = s"${historyServer.host}:${historyServer.port}"

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSources.map(_._2.getEventSourceDetail)

  override def getSourceDetail(id: EventSourceIdentifier): EventSourceDetail = eventSources(id).getEventSourceDetail

  override def getScrollingSource(id: EventSourceIdentifier): FreeScrollEventSource = eventSources(id).asInstanceOf[FreeScrollEventSource]

  override def containsEventSource(id: EventSourceIdentifier): Boolean = eventSources.contains(id)
}

case class HistoryServerApi(host: String, port: Int, name: String)
