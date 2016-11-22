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

import java.io.File

import com.groupon.sparklint.SparklintServer._
import com.groupon.sparklint.common.Logging

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * The production implementation of EventSourceManagerLike. Manages and abstracts EventSourceLike instances
  * server side.
  *
  * @author swhitear 
  * @since 8/18/16.
  */
abstract class EventSourceManager[CtorT] extends EventSourceManagerLike[CtorT] with Logging {

  // this sync'ed LinkedHashMap is necessary because we want to ensure ordering of items in the manager, not the UI.
  // insertion order works well enough here, we have no need for any other guarantees from the data structure.
  private val eventSourcesByAppId = new mutable.LinkedHashMap[String, EventSourceDetail]()
                                        with mutable.SynchronizedMap[String, EventSourceDetail]

  def constructDetails(eventSourceCtor: CtorT): Option[EventSourceDetail]

  override def addEventSource(eventSourceCtor: CtorT): Option[EventSourceLike] = {
    constructDetails(eventSourceCtor) match {
      case Some(detail) =>
        eventSourcesByAppId.put(detail.id, detail)
        detail.source
      case None         => None
    }
  }

  override def sourceCount: Int = eventSourcesByAppId.size

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSourcesByAppId.values

  override def containsAppId(appId: String): Boolean = eventSourcesByAppId.contains(appId)

  @throws[NoSuchElementException]
  override def getSourceDetail(appId: String): EventSourceDetail = eventSourcesByAppId(appId)

  @throws[NoSuchElementException]
  override def getScrollingSource(appId: String): FreeScrollEventSource = {
    eventSourcesByAppId.get(appId) match {
      case Some(EventSourceDetail(source: FreeScrollEventSource, _, _)) => source
      case Some(_)                                                      => scrollFail(appId)
      case None                                                         => idFail(appId)
    }
  }

  private def scrollFail(appId: String) = throw new IllegalArgumentException(s"$appId cannot free scroll")

  private def idFail(appId: String) = throw new NoSuchElementException(s"Missing appId $appId")
}


class FileEventSourceManager extends EventSourceManager[File] {

  lazy val progressTracker = new EventProgressTracker()

  lazy val stateManager: EventStateManagerLike = new LosslessEventStateManager()

  override def constructDetails(sourceFile: File): Option[EventSourceDetail] = {
    Try {
      FileEventSource(sourceFile, Seq(progressTracker, stateManager))
    } match {
      case Success(eventSource) =>
        logInfo(s"Successfully created file source ${sourceFile.getName}")
        Some(EventSourceDetail(eventSource, progressTracker, stateManager))
      case Failure(ex)          =>
        logWarn(s"Failure creating file source from ${sourceFile.getName}: ${ex.getMessage}")
        None
    }
  }
}