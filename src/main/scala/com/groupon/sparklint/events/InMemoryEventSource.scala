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

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.scheduler.SparkListenerEvent

import scala.concurrent.Future


/**
  * @author rxue,swhitear
  * @since 8/18/16.
  */
case class InMemoryEventSource(identifier: EventSourceIdentifier, appName: String) extends EventSourceLike {
  private val _buffer: BlockingQueue[SparkListenerEvent] = new LinkedBlockingDeque()
  private val _state = new CompressedStateManager()
  private val _meta = new EventSourceMeta(identifier, appName)
  private val _progress = new EventProgressTracker()
  private var _running = false

  override def friendlyName: String = appName

  override protected def receivers: Seq[EventReceiverLike] = Seq(_state, _meta, _progress)

  override def state: EventStateManagerLike = _state

  override def meta: EventSourceMetaLike = _meta

  override def progress: EventProgressTrackerLike = _progress

  def push(event: SparkListenerEvent): Unit = {
    _buffer.add(event)
  }

  def startConsuming() = Future {
    _running = true
    while (_running) {
      val event = _buffer.take()
      receivers.foreach(r => {
        r.preprocess(event)
        r.onEvent(event)
      })
    }
  }

  def pauseConsuming(): Unit = {
    _running = false
  }

  override def getEventSourceDetail: EventSourceDetail = EventSourceDetail(meta, progress, state)
}
