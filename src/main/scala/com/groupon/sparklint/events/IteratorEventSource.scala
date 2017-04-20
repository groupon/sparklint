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

import com.groupon.sparklint.data.SparklintStateLike
import org.apache.spark.scheduler.SparkListenerEvent

import scala.collection.mutable

/**
  * @param appMeta         the meta data of the app
  * @param inputIterator   the source of the event stream
  * @param compressStorage use a compressed storage (less memory required, can lose resolution)
  * @author rxue
  * @since 1.0.5
  */
class IteratorEventSource(val uuid: UUID, val appMeta: EventSourceMeta, inputIterator: Iterator[SparkListenerEvent], compressStorage: Boolean) extends FreeScrollEventSource {
  override val progressTracker: EventProgressTracker = new EventProgressTracker()
  private val processedMessage = mutable.Stack[SparkListenerEvent]()
  private val unprocessedMessage = mutable.Stack[SparkListenerEvent]()
  private val stateManager = if (compressStorage) new CompressedStateManager() else new LosslessStateManager()
  private val receivers = Seq(appMeta, progressTracker, stateManager)

  override def appState: SparklintStateLike = stateManager.getState

  override def toStart(): Unit = {
    while (rewind()) {}
  }

  override def toEnd(): Unit = {
    while (forward()) {}
  }

  def forward(): Boolean = synchronized {
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

  override def forwardEvents(count: Int): Unit = {
    val currentEvents = progressTracker.eventProgress.complete
    while (progressTracker.eventProgress.complete - currentEvents < count && hasNext) {
      forward()
    }
  }

  override def forwardJobs(count: Int): Unit = {
    val currentJobs = progressTracker.jobProgress.complete
    while (progressTracker.jobProgress.complete - currentJobs < count && hasNext) {
      forward()
    }
  }

  override def forwardStages(count: Int): Unit = {
    val currentStages = progressTracker.stageProgress.complete
    while (progressTracker.stageProgress.complete - currentStages < count && hasNext) {
      forward()
    }
  }

  override def forwardTasks(count: Int): Unit = {
    val currentTasks = progressTracker.taskProgress.complete
    while (progressTracker.taskProgress.complete - currentTasks < count && hasNext) {
      forward()
    }
  }

  override def rewindEvents(count: Int): Unit = {
    val currentEvents = progressTracker.eventProgress.complete
    while (currentEvents - progressTracker.eventProgress.complete < count && hasPrevious) {
      rewind()
    }
  }

  def rewind(): Boolean = synchronized {
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

  override def hasPrevious: Boolean = {
    processedMessage.nonEmpty
  }

  override def rewindJobs(count: Int): Unit = {
    val currentJobs = progressTracker.jobProgress.complete
    while (currentJobs - progressTracker.jobProgress.complete < count && hasPrevious) {
      rewind()
    }
  }

  override def rewindStages(count: Int): Unit = {
    val currentStages = progressTracker.stageProgress.complete
    while (currentStages - progressTracker.stageProgress.complete < count && hasPrevious) {
      rewind()
    }
  }

  override def rewindTasks(count: Int): Unit = {
    val currentTasks = progressTracker.taskProgress.complete
    while (currentTasks - progressTracker.taskProgress.complete < count && hasPrevious) {
      rewind()
    }
  }
}
