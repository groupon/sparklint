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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author rxue
  * @since 2/5/17.
  */
class RootEventSourceManager extends EventSourceManagerLike {
  private val _eventSourceManagers: mutable.Buffer[EventSourceManagerLike] = new ArrayBuffer[EventSourceManagerLike] with mutable.SynchronizedBuffer[EventSourceManagerLike]

  override def sourceCount: Int = eventSourceManagers.map(_.sourceCount).sum

  override def displayName: String = "Root"

  override def displayDetails: String = ""

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSourceManagers.flatMap(_.eventSourceDetails)

  override def getSourceDetail(id: EventSourceIdentifier): EventSourceDetail = eventSourceManagers.find(_.containsEventSource(id)) match {
    case Some(sourceManager) => sourceManager.getSourceDetail(id)
    case None                => throw new NoSuchElementException
  }

  override def getScrollingSource(id: EventSourceIdentifier): FreeScrollEventSource = eventSourceManagers.find(_.containsEventSource(id)) match {
    case Some(sourceManager) => sourceManager.getScrollingSource(id)
    case None                => throw new NoSuchElementException
  }

  override def containsEventSource(id: EventSourceIdentifier): Boolean = eventSourceManagers.exists(_.containsEventSource(id))

  def eventSourceManagers: Seq[EventSourceManagerLike] = _eventSourceManagers

  def addDirectory(directoryEventSourceManager: DirectoryEventSourceManager): Unit = {
    _eventSourceManagers += directoryEventSourceManager
  }

  def addHistoryServer(historyServerEventSourceManager: HistoryServerEventSourceManager): Unit = {
    _eventSourceManagers += historyServerEventSourceManager
  }

  protected def addFile(singleFileEventSourceManager: SingleFileEventSourceManager): Unit = {
    _eventSourceManagers += singleFileEventSourceManager
  }

  def addFile(fileEventSource: FileEventSource): Unit = {
    addFile(SingleFileEventSourceManager(fileEventSource))
  }

  protected def addInMemory(inMemoryEventSourceManager: InMemoryEventSourceManager): Unit = {
    _eventSourceManagers += inMemoryEventSourceManager
  }

  def addInMemory(inMemoryEventSource: InMemoryEventSource): Unit = {
    addInMemory(InMemoryEventSourceManager(inMemoryEventSource))
  }
}
