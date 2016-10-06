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

/**
  * Implementations of this trait are capable of managing the list of event sources for a specific configuration of
  * Sparklint.
  *
  * @author swhitear 
  * @since 9/13/16.
  */
trait EventSourceManagerLike {

  /**
    * Adds an EventSourceLike instance to the manager.
    *
    * @param eventSource the EventSourceLike extending implementation to add.
    */
  def addEventSource(eventSource: EventSourceLike): Unit

  /**
    * The number of sources currently in the manager.
    *
    * @return The count of EventSourceLike instances in the manager.
    */
  def sourceCount: Int

  /**
    * An Iterable of EventSourceLike instances returned in their insertion order.
    *
    * @return
    */
  def eventSources: Iterable[EventSourceLike]

  /** True if the current set of managed EventSourceLike instances contains the specified appId.
    *
    * @param appId The appId to check for.
    * @return True if it exists, false otherwise.
    */
  def containsAppId(appId: String): Boolean

  /**
    * Provides indexed access to the EventSourceLike instances by appId.
    *
    * @param appId The appId of the EventSourceLike instance to return.
    * @throws NoSuchElementException When the specified appId does not exist.
    * @return The specified EventSourceLike instance.
    */
  @throws[NoSuchElementException]
  def apply(appId: String): EventSourceLike

  /**
    * Forwards the EventSourceLike instance matching the specified appId by the specified count amount.
    *
    * @param appId The appId of the EventSourceLike instance to forward.
    * @param count The number of times to advance.
    * @throws NoSuchElementException When the specified appId does not exist, or advancing by the specified
    *                                count exceeds the upper limit of the EventSourceLike.
    * @return An EventSourceProgress instance representing the the current progress state of the EventSourceLike
    *         instance.
    */
  @throws[NoSuchElementException]
  def forwardApp(appId: String, count: Int): EventSourceProgress

  /**
    * Rewinds the EventSourceLike instance matching the specified appId by the specified count amount.
    *
    * @param appId The appId of the EventSourceLike instance to rewind.
    * @param count The number of times to rewind.
    * @throws NoSuchElementException When the specified appId does not exist, or rewinding by the specified
    *                                count exceeds the lower limit of the EventSourceLike.
    * @return An EventSourceProgress instance representing the the current progress state of the EventSourceLike
    *         instance.
    */
  @throws[NoSuchElementException]
  def rewindApp(appId: String, count: Int): EventSourceProgress

  @throws[NoSuchElementException]
  def endApp(appId: String): EventSourceProgress

  @throws[NoSuchElementException]
  def startApp(appId: String): EventSourceProgress

  def getCanFreeScrollEventSource(appId: String): CanFreeScroll
}
