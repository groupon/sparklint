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

package com.groupon.sparklint.event

import org.apache.spark.scheduler.SparkListenerEvent

import scala.collection.mutable

/**
  * @author rxue
  * @since 1.0.5
  */
class IteratorEventSource(val appMeta: SparkAppMeta, inputIterator: Iterator[SparkListenerEvent]) extends EventSource {
  private val processedMessage = mutable.Stack[SparkListenerEvent]()
  private val unprocessedMessage = mutable.Stack[SparkListenerEvent]()

  def nextEvent(): Boolean = {
    if (unprocessedMessage.isEmpty) {
      if (inputIterator.hasNext) {
        processedMessage.push(inputIterator.next())
        true
      } else {
        false
      }
    } else {
      processedMessage.push(unprocessedMessage.pop())
      true
    }
  }

  def previous(): Boolean = {
    if (processedMessage.isEmpty) {
      false
    } else {
      unprocessedMessage.push(processedMessage.pop())
      true
    }
  }
}
