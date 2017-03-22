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
  * Implementations of this trait are capable of managing the list of event sources for a specific configuration of
  * Sparklint.
  *
  * @author swhitear
  * @since 9/13/16.
  */
trait EventSourceManagerLike {

  /**
    * Identifier for the [[EventSourceManagerLike]]
    * @return
    */
  def uuid: UUID

  /**
    * The number of [[EventSourceLike]] currently in the manager.
    *
    * @return The count of EventSourceLike instances in the manager.
    */
  def sourceCount: Int

  /**
    * The name of this event source manager shown in UI
    * @return
    */
  def displayName: String

  /**
    * The supplementary information of this event source manager shown in UI
    * @return
    */
  def displayDetails: String

  /**
    * An Iterable of [[EventSourceDetail]] instances returned in their insertion order.
    *
    * @return
    */
  def eventSourceDetails: Iterable[EventSourceDetail]

  /** True if the current set of managed [[EventSourceLike]] instances contains the specified string representation of [[EventSourceIdentifier]].
    *
    * @param id The eventSourceId to check for.
    * @return True if it exists, false otherwise.
    */
  def containsEventSource(id: String): Boolean

  /**
    * Provides indexed access to the [[EventSourceDetail]] instances by meta.
    *
    * @param id The id of the EventSourceDetail instance to return.
    * @throws NoSuchElementException When the specified meta does not exist.
    * @return The specified EventSourceDetail instance wrapping hte EventSource and associated receivers.
    */
  @throws[NoSuchElementException]
  def getSourceDetail(id: String): EventSourceDetail

  /**
    * Provides indexed access to any wrapped [[EventSourceLike]] instances that extend [[FreeScrollEventSource]].
    *
    * @param id The id of the EventSourceDetail instance to return.
    * @throws NoSuchElementException When the specified meta does not exist.
    * @return
    */
  @throws[NoSuchElementException]
  def getScrollingSource(id: String): FreeScrollEventSource
}
