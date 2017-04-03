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

package com.groupon.sparklint.event

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.events.{CompressedStateManager, EventProgressTracker}
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.SparkListenerEvent

/**
  * @author Roboxue
  */
class ListenerEventSource(appId: String, appName: String) extends SparkFirehoseListener with EventSource {
  override val progressTracker: EventProgressTracker = new EventProgressTracker()
  override val appMeta: SparkAppMeta = SparkAppMeta(Some(appId), None, appName, None, System.currentTimeMillis())
  private val buffer: BlockingQueue[SparkListenerEvent] = new LinkedBlockingDeque()
  private val stateManager = new CompressedStateManager()
  private val receivers = Seq(appMeta, progressTracker, stateManager)

  override def appState: SparklintStateLike = stateManager.getState

  override def onEvent(event: SparkListenerEvent): Unit = {
    buffer.add(event)
  }

  override def hasNext: Boolean = true

  override def forward(): Boolean = {
    val event = buffer.take()
    receivers.foreach(r => {
      r.preprocess(event)
      r.onEvent(event)
    })
    true
  }
}
