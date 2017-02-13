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


import java.util.UUID

import scala.collection.mutable
import scala.util.Try

/**
  * @author rxue
  * @since 2/5/17.
  */
case class HistoryServerEventSourceManager(historyServer: HistoryServerApi, uuid: UUID = UUID.randomUUID()) extends EventSourceManagerLike {
  private val eventSources: mutable.Map[String, EventSourceLike] = new mutable.LinkedHashMap[String, EventSourceLike] with mutable.SynchronizedMap[String, EventSourceLike]

  def availableEventSources: Seq[EventSourceMeta] = ???

  // TODO: pull available logs from history api
  def pull(): Unit = ???

  def pullEventSource(id: String): Try[Unit] = Try {
    // TODO: download event logs
  }

  override def sourceCount: Int = eventSources.size

  override def displayName: String = historyServer.name

  override def displayDetails: String = s"${historyServer.host}:${historyServer.port}"

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSources.map(_._2.getEventSourceDetail)

  override def getSourceDetail(id: String): EventSourceDetail = eventSources(id).getEventSourceDetail

  override def getScrollingSource(id: String): FreeScrollEventSource = eventSources(id).asInstanceOf[FreeScrollEventSource]

  override def containsEventSource(id: String): Boolean = eventSources.contains(id)
}

case class HistoryServerApi(host: String, port: Int, name: String)