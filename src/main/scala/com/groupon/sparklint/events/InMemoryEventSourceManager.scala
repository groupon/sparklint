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
case class InMemoryEventSourceManager(inMemoryEventSource: InMemoryEventSource, uuid: UUID = UUID.randomUUID()) extends EventSourceManagerLike {

  override def sourceCount: Int = 1

  override def displayName: String = inMemoryEventSource.identifier.appId

  override def displayDetails: String = {
    inMemoryEventSource.identifier.attemptId match {
      case Some(attemptId) =>
        s"${inMemoryEventSource.identifier.appId}-attempt$attemptId"
      case None            =>
        inMemoryEventSource.identifier.appId
    }
  }

  override def eventSourceDetails: Iterable[EventSourceDetail] = Iterable(inMemoryEventSource.getEventSourceDetail)

  override def containsEventSource(id: String): Boolean = inMemoryEventSource.identifier.toString == id

  override def getSourceDetail(id: String): EventSourceDetail = if (inMemoryEventSource.identifier.toString == id) {
    inMemoryEventSource.getEventSourceDetail
  } else {
    throw new NoSuchElementException
  }

  override def getScrollingSource(id: String): FreeScrollEventSource = {
    throw new NoSuchElementException // InMemoryEventSource is not scrollable
  }
}
