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
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import com.groupon.sparklint.data.SparklintStateLike
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.SparkListenerEvent

/**
  * @author rxue
  * @since 1.0.5
  */
class ListenerEventSource(appId: String, appName: String) extends SparkFirehoseListener with EventSource {
  override val uuid: UUID = UUID.randomUUID()
  override val progressTracker: EventProgressTracker = new EventProgressTracker()
  override val appMeta: EventSourceMeta = EventSourceMeta(Some(appId), None, appName, None)
  private val buffer: BlockingQueue[SparkListenerEvent] = new LinkedBlockingDeque()
  private val stateManager = new CompressedStateManager()
  private val receivers = Seq(appMeta, progressTracker, stateManager)

  override def appState: SparklintStateLike = stateManager.getState

  override def onEvent(event: SparkListenerEvent): Unit = {
    buffer.add(event)
  }

  // Set hasNext to false to indicate this event source doesn't support free scrolling
  override def hasNext: Boolean = false

  override def forward(): Boolean = {
    val event = buffer.take()
    receivers.foreach(r => {
      r.preprocess(event)
      r.onEvent(event)
    })
    true
  }

  def start(): Unit = {
    while(true) {
      forward()
    }
  }
}
