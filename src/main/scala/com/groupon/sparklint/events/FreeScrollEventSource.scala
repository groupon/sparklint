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
  * An extension of EventSourceLike that provides two way scrolling by various event types.
  *
  * @author swhitear
  * @since 8/18/16.
  */
trait FreeScrollEventSource {
  self: EventSourceLike =>

  @throws[IllegalArgumentException]
  def forwardEvents(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def rewindEvents(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def forwardTasks(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def rewindTasks(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def forwardStages(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def rewindStages(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def forwardJobs(count: Int = 1): EventSourceProgress

  @throws[IllegalArgumentException]
  def rewindJobs(count: Int = 1): EventSourceProgress

  def toEnd(): EventSourceProgress

  def toStart(): EventSourceProgress
}

