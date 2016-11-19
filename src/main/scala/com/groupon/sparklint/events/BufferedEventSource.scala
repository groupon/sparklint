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
package com.groupon.sparklint.events

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import com.groupon.sparklint.SparklintServer._
import org.apache.spark.scheduler.SparkListenerEvent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * An EventSource that uses an intermediary queue to buffer live events before updating receivers.
  *
  * @author swhitear 
  * @since 8/18/16.
  */
case class BufferedEventSource(appId: String, progress: EventSourceProgress, stateManager: EventStateManagerLike)
  extends EventSourceBase {

  val buffer: BlockingQueue[SparkListenerEvent] = new LinkedBlockingDeque()

  def push(event: SparkListenerEvent): Unit = {
    buffer.add(event)
  }

  def startConsuming() = Future {
    while (true) {
      val event = buffer.take()
      receivers.foreach(r => {
        r.preprocess(event)
        r.onEvent(event)
      })
    }
    // TODO make cancelable
  }
}

object BufferedEventSource {
  def apply(sourceId: String): BufferedEventSource = {
    val progressReceiver = new EventSourceProgress()
    val stateReceiver = new CompressedStateManager()
    val eventSource = BufferedEventSource(sourceId, progressReceiver, stateReceiver)
    eventSource.startConsuming()
    logInfo(s"Successfully created buffered source $sourceId")
    eventSource
  }
}
