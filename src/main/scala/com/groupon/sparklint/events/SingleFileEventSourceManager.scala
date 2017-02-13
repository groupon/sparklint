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

/**
  * @author rxue
  * @since 2/5/17.
  */
case class SingleFileEventSourceManager(fileEventSource: FileEventSource, uuid: UUID = UUID.randomUUID()) extends EventSourceManagerLike {

  override def sourceCount: Int = 1

  override def displayName: String = fileEventSource.file.getName

  override def displayDetails: String = fileEventSource.file.getParentFile.getCanonicalPath

  override def containsEventSource(id: String): Boolean = fileEventSource.identifier.toString == id

  override def eventSourceDetails: Iterable[EventSourceDetail] = Iterable(fileEventSource.getEventSourceDetail)

  override def getSourceDetail(id: String): EventSourceDetail = if (fileEventSource.identifier.toString == id) {
    fileEventSource.getEventSourceDetail
  } else {
    throw new NoSuchElementException
  }

  override def getScrollingSource(id: String): FreeScrollEventSource = if (fileEventSource.identifier.toString == id) {
    fileEventSource
  } else {
    throw new NoSuchElementException
  }
}
