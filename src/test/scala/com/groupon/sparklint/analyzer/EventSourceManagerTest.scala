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
package com.groupon.sparklint.analyzer

import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.data.compressed.CompressedState
import com.groupon.sparklint.events._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear
  * @since 9/13/16.
  */
class EventSourceManagerTest extends FlatSpec with Matchers {

  it should "initialize empty if no initial sources supplied" in {
    val manager = new EventSourceManager()

    manager.sourceCount shouldEqual 0
    manager.eventSources.isEmpty shouldBe true
  }

  it should "initialize with start state if initial sources supplied" in {
    val evSource = StubEventSource("test_app_id")
    val manager = new EventSourceManager(evSource)

    manager.sourceCount shouldEqual 1
    manager.eventSources.head shouldBe evSource
    manager.containsAppId("test_app_id") shouldBe true
    manager.getSource("test_app_id") shouldBe evSource
  }

  it should "add the extra event sources as expected and remain in order" in {
    val evSource1 = StubEventSource("test_app_id_1")
    val evSource2 = StubEventSource("test_app_id_2")
    val manager = new EventSourceManager(evSource2)
    manager.addEventSource(evSource1)

    manager.sourceCount shouldEqual 2
    manager.eventSources.toSet shouldBe Set(evSource1, evSource2)
    manager.containsAppId("test_app_id_1") shouldBe true
    manager.containsAppId("test_app_id_2") shouldBe true
    manager.getSource("test_app_id_1") shouldBe evSource1
    manager.getSource("test_app_id_2") shouldBe evSource2
  }

  it should "throw up when invalid appId specified for indexer" in {
    val evSource = StubEventSource("test_app_id")
    val manager = new EventSourceManager(evSource)

    intercept[NoSuchElementException] {
      manager.getSource("invalid_app_id")
    }
  }
}


case class StubEventSource(appId: String) extends EventSourceLike with FreeScrollEventSource {
  override def version: String = ???

  override def host: String = ???

  override def port: Int = ???

  override def maxMemory: Long = ???

  override def appName: String = ???

  override def user: String = ???

  override def startTime: Long = ???

  override def endTime: Long = ???

  override def progress: EventSourceProgress = ???

  override def state: SparklintStateLike = ???

  override def fullName: String = ???

  @throws[IllegalArgumentException]
  override def forwardEvents(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def rewindEvents(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def forwardTasks(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def rewindTasks(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def forwardStages(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def rewindStages(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def forwardJobs(count: Int): EventSourceProgress = ???

  @throws[IllegalArgumentException]
  override def rewindJobs(count: Int): EventSourceProgress = ???

  override def toEnd(): EventSourceProgress = ???

  override def toStart(): EventSourceProgress = ???
}