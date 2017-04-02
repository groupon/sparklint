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

import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.events.{EventProgressTracker, LosslessStateManager}
import org.apache.spark.scheduler.SparkListenerEvent

import scala.collection.mutable

/**
  * @author rxue
  * @since 1.0.5
  */
class IteratorEventSource(val appMeta: SparkAppMeta, inputIterator: Iterator[SparkListenerEvent]) extends EventSource
  with FreeScrollEventSource {
  override val progressTracker: EventProgressTracker = new EventProgressTracker()
  private val processedMessage = mutable.Stack[SparkListenerEvent]()
  private val unprocessedMessage = mutable.Stack[SparkListenerEvent]()
  private val stateManager = new LosslessStateManager()
  private val receivers = Seq(progressTracker, stateManager)

  override def appState: SparklintStateLike = stateManager.getState

  def processNext(): Boolean = synchronized {
    if (hasNext) {
      val event = if (unprocessedMessage.nonEmpty) {
        unprocessedMessage.pop()
      } else {
        val e = inputIterator.next()
        receivers.foreach(r => {
          r.preprocess(e)
        })
        e
      }
      receivers.foreach(r => {
        r.onEvent(event)
      })
      processedMessage.push(event)
      true
    } else {
      false
    }
  }

  def hasNext: Boolean = {
    unprocessedMessage.nonEmpty || inputIterator.hasNext
  }

  def processPrevious(): Boolean = synchronized {
    if (hasPrevious) {
      val event = processedMessage.pop()
      receivers.foreach(r => {
        r.unEvent(event)
      })
      unprocessedMessage.push(event)
      true
    } else {
      false
    }
  }

  def hasPrevious: Boolean = {
    processedMessage.nonEmpty
  }
}
