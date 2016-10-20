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

import com.groupon.sparklint.data.SparklintStateLike
import org.apache.spark.scheduler.SparkListenerEvent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  *
  * @author swhitear 
  * @since 8/18/16.
  */
case class BufferedEventSource(appId: String, eventState: EventStateLike)
  extends EventSourceBase(eventState) with EventSourceLike {
  val buffer: BlockingQueue[SparkListenerEvent] = new LinkedBlockingDeque()

  def push(event: SparkListenerEvent): Unit = {
    buffer.add(event)
  }

  private var processed: Int = 0

  def runnit() = Future {
    while (true) {
      eventState.onEvent(buffer.take())
      processed += 1
    }
    // TODO make cancelable
  }

  def current(): SparkListenerEvent = buffer.peek()

  override def progress: EventSourceProgress = EventSourceProgress(buffer.size() + processed, processed)

  override def state: SparklintStateLike = eventState.getState

}
