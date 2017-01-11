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

import java.io.File

import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Task
import scalaz.{-\/, \/-}

/**
  * A specific implementation of EventSourceManager that creates FileEventSources with Losslesss state management.
  *
  * @author swhitear
  * @since 11/21/16.
  */
class FileEventSourceManager extends EventSourceManager {
  def newStateManager: EventStateManagerLike with EventReceiverLike = new LosslessStateManager()

  def addRemoteFile(name: String, task: Task[Array[Char]]): Option[EventSourceLike] = {
    val meta = new EventSourceMeta()
    val progress = new EventProgressTracker()
    val stateManager = newStateManager

    task.attemptRun match {
      case -\/(ex)       =>
        logWarn(s"Failure creating history source for $name: ${ex.getMessage}")
        None
      case \/-(contents) =>
        logInfo(s"Successfully created file source $name")
        val eventSource = HistoryEventSource(name, contents,
          Seq(meta, progress, stateManager))
        val detail = EventSourceDetail(name, meta, progress, stateManager)
        Some(addEventSourceAndDetail(SourceAndDetail(eventSource, detail)))
    }
  }

  def addFile(sourceFile: File): Option[EventSourceLike] = {
    val meta = new EventSourceMeta()
    val progress = new EventProgressTracker()
    val stateManager = newStateManager

    Try {
      FileEventSource(sourceFile, Seq(meta, progress, stateManager))
    } match {
      case Success(eventSource) =>
        logInfo(s"Successfully created file source ${sourceFile.getName}")
        val detail = EventSourceDetail(eventSource.eventSourceId, meta, progress, stateManager)
        Some(addEventSourceAndDetail(SourceAndDetail(eventSource, detail)))
      case Failure(ex)          =>
        logWarn(s"Failure creating file source from ${sourceFile.getName}: ${ex.getMessage}")
        None
    }
  }
}
