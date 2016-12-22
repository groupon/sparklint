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
  * @author swhitear 
  * @since 11/16/16.
  */
case class StubEventSource(eventSourceId: String, receivers: Seq[EventReceiverLike])
  extends FreeScrollEventSource {

  @throws[IllegalArgumentException]
  override def forwardEvents(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindEvents(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def forwardTasks(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindTasks(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def forwardStages(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindStages(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def forwardJobs(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindJobs(count: Int): Unit = ???

  override def toEnd(): Unit = ???

  override def toStart(): Unit = ???

  override def hasNext: Boolean = ???

  override def hasPrevious: Boolean = ???
}
