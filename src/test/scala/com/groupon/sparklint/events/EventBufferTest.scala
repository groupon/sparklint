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

import java.util.function.Consumer

import com.groupon.sparklint.common.TestUtils
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerStageSubmitted}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/13/16.
  */
class EventBufferTest extends FlatSpec with Matchers {

  it should "initialize fine with empty buffer" in {
    val buffer = new EventBuffer(IndexedSeq.empty)

    buffer.nextIndex shouldEqual 0
    buffer.previousIndex shouldEqual -1
    buffer.hasNext shouldBe false
    buffer.hasPrevious shouldBe false
    buffer.eventCount shouldEqual 0
    buffer.index shouldEqual 0

    intercept[NoSuchElementException] {
      buffer.next()
    }

    intercept[NoSuchElementException] {
      buffer.previous()
    }
  }

  it should "not have set and add functions implemented yet" in {
    val buffer = new EventBuffer(IndexedSeq.empty)

    intercept[NotImplementedError] {
      buffer.set(TestUtils.sparkStageSubmittedEvent(42, "test_stage"))
    }

    intercept[NotImplementedError] {
      buffer.add(TestUtils.sparkStageSubmittedEvent(4242, "test_stage"))
    }
  }

  it should "allow for iteration forward and backward through the buffer" in {
    val buffer = new EventBuffer(genEvents(100))

    while (buffer.hasNext) {
      val index = buffer.nextIndex
      buffer.next.asInstanceOf[SparkListenerStageSubmitted].stageInfo.stageId shouldEqual index
    }

    while (buffer.hasPrevious) {
      val index = buffer.previousIndex
      buffer.previous.asInstanceOf[SparkListenerStageSubmitted].stageInfo.stageId shouldEqual index
    }
  }

  private def genEvents(count: Int) = {
    (0 until count).map(i => TestUtils.sparkStageSubmittedEvent(i, "test_stage"))
  }

  class ConsoleBufferConsumer extends Consumer[SparkListenerEvent] {
    override def accept(ev: SparkListenerEvent): Unit = println(ev.toString)
  }
}
