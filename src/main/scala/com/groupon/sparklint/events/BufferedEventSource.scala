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

import java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}

import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerExecutorAdded}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * An EventSource that uses an intermediary queue to buffer live events before updating receivers.
  * @author swhitear 
  * @since 8/18/16.
  */
case class BufferedEventSource(appId: String, receivers: Seq[EventReceiverLike])
  extends EventSourceBase {

  val buffer: BlockingDeque[SparkListenerEvent] = new LinkedBlockingDeque()

  def push(event: SparkListenerEvent): Unit = {
    buffer.offerLast(event)
  }

  private var processed: Int = 0

  def startConsuming() = Future {
    while (true) {
      val event = buffer.takeFirst()
      if (processed == 0) {
        event match {
          case start: SparkListenerExecutorAdded =>
            startTimeOpt = Some(start.time)
        }
      }
      receivers.foreach(r => {
        r.preprocess(event)
        r.onEvent(event)
      })
      processed += 1
    }
    // TODO make cancelable
  }

}
