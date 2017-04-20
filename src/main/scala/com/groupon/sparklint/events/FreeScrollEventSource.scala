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

/**
  * @author rxue
  * @since 1.0.5
  */
trait FreeScrollEventSource extends EventSource {
  def rewind(): Boolean

  //noinspection AccessorLikeMethodIsUnit
  def toStart(): Unit

  //noinspection AccessorLikeMethodIsUnit
  def toEnd(): Unit

  def forwardEvents(count: Int): Unit

  def forwardJobs(count: Int): Unit

  def forwardStages(count: Int): Unit

  def forwardTasks(count: Int): Unit

  def rewindEvents(count: Int): Unit

  def rewindJobs(count: Int): Unit

  def rewindStages(count: Int): Unit

  def rewindTasks(count: Int): Unit
}
