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

package com.groupon.sparklint.actors

import akka.actor.ActorSystem
import akka.actor.FSM.{CurrentState, Transition, UnsubscribeTransitionCallBack}
import akka.testkit.{DefaultTimeout, TestKit}
import org.scalatest.{FeatureSpecLike, Matchers}

/**
  * @author rxue
  * @since 6/2/17.
  */
class SparklintAppLogReaderTest extends TestKit(ActorSystem("test"))
  with FeatureSpecLike
  with DefaultTimeout
  with FileBasedSparkLogTest
  with Matchers {

  import SparklintAppLogReader._

  feature("initialize") {
    scenario("normally") {
      val reader = initializeSampleLog()
      expectMsg(CurrentState(reader, Unstarted))
      expectMsg(Transition(reader, Unstarted, Initializing))
      expectMsg(Transition(reader, Initializing, Paused))
      reader ! UnsubscribeTransitionCallBack(testActor)
    }
  }

  feature("read entire file") {
    scenario("normally") {
      val reader = initializeSampleLog()
      expectMsg(CurrentState(reader, Unstarted))
      expectMsg(Transition(reader, Unstarted, Initializing))
      expectMsg(Transition(reader, Initializing, Paused))
      reader ! ResumeReading
      expectMsg(Transition(reader, Paused, Started))
      expectMsg(Transition(reader, Started, Finished))
      reader ! UnsubscribeTransitionCallBack(testActor)
    }
  }
}
