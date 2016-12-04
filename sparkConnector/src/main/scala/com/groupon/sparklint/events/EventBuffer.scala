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

import java.util

import org.apache.spark.scheduler.SparkListenerEvent

/**
  * The EventBuffer provides an abstraction around a buffered set of SparkListenerEvent's and provides an extension
  * of the Java ListIterator[T} interface allowing for traversal forward and backwards through the events.
  *
  * @author swhitear 
  * @since 9/10/16.
  */
class EventBuffer(buffer: IndexedSeq[SparkListenerEvent])
  extends util.ListIterator[SparkListenerEvent] {

  var index     : Int = 0
  val eventCount: Int = buffer.length

  override def next: SparkListenerEvent = {
    if (index == eventCount) throw new NoSuchElementException()
    val returnIndex = index
    index = index + 1
    buffer(returnIndex)
  }

  override def set(e: SparkListenerEvent): Unit = ???

  override def nextIndex: Int = index

  override def previousIndex: Int = index - 1

  override def hasNext: Boolean = index < eventCount

  override def hasPrevious: Boolean = index > 0

  override def add(e: SparkListenerEvent): Unit = ???

  override def previous: SparkListenerEvent = {
    if (index == 0) throw new NoSuchElementException()
    index = index - 1
    buffer(index)
  }
}
