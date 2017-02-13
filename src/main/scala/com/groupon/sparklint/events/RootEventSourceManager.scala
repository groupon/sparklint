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

import scala.collection.immutable.SortedMap

/**
  * @author rxue
  * @since 2/5/17.
  */
class RootEventSourceManager {
  private var _eventSourceManagers: SortedMap[UUID, EventSourceManagerLike] = SortedMap.empty

  def eventSourceManagers: Seq[EventSourceManagerLike] = _eventSourceManagers.toSeq.map(_._2)

  def eventSourceManager(uuid: String): Option[EventSourceManagerLike] = _eventSourceManagers.get(UUID.fromString(uuid))

  def addDirectory(esm: DirectoryEventSourceManager): Unit = {
    _eventSourceManagers += esm.uuid -> esm
  }

  def addHistoryServer(esm: HistoryServerEventSourceManager): Unit = {
    _eventSourceManagers += esm.uuid -> esm
  }

  protected def addFile(esm: SingleFileEventSourceManager): Unit = {
    _eventSourceManagers += esm.uuid -> esm
  }

  def addFile(fileEventSource: FileEventSource): Unit = {
    addFile(SingleFileEventSourceManager(fileEventSource))
  }

  protected def addInMemory(esm: InMemoryEventSourceManager): Unit = {
    _eventSourceManagers += esm.uuid -> esm
  }

  def addInMemory(inMemoryEventSource: InMemoryEventSource): Unit = {
    addInMemory(InMemoryEventSourceManager(inMemoryEventSource))
  }
}
