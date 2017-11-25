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
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import org.scalatest.{FeatureSpecLike, Matchers}

import scala.collection.SortedMap

/**
  * @author rxue
  * @since 6/4/17.
  */
class JobSinkTest extends TestKit(ActorSystem("MySpec"))
  with FeatureSpecLike
  with ImplicitSender
  with DefaultTimeout
  with FileBasedSparkLogTest
  with Matchers {

  import JobSink._

  feature("GetCoreUsageByLocality") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / SparklintLogProcessor.name
      val jobSink = system.actorSelection(logProcessorPath / name)
      jobSink ! GetCoreUsageByLocality(41662, None, None)
      expectMsg(CoreUsageResponse(
        SortedMap(
          1466087848562L -> UsageByGroup(Map("RACK_LOCAL" -> 1, "ANY" -> 3), 0),
          1466087890224L -> UsageByGroup(Map("ANY" -> 4), 0),
          1466087931886L -> UsageByGroup(Map("ANY" -> 2), 2),
          1466087973548L -> UsageByGroup(Map("ANY" -> 1), 3),
          1466088015210L -> UsageByGroup(Map(), 4),
          1466088056872L -> UsageByGroup(Map(), 0)
        )))
    }
  }

  feature("GetCoreUtilizationByLocality") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / SparklintLogProcessor.name
      val jobSink = system.actorSelection(logProcessorPath / name)
      jobSink ! GetCoreUtilizationByLocality(None, None)
      expectMsgPF() {
        case CoreUtilizationResponse(usageByLocality) =>
          (usageByLocality("PROCESS_LOCAL") * 100).round.toInt shouldBe 0
          (usageByLocality("NODE_LOCAL") * 100).round.toInt shouldBe 1
          (usageByLocality("RACK_LOCAL") * 100).round.toInt shouldBe 6
          (usageByLocality("NO_PREF") * 100).round.toInt shouldBe 0
          (usageByLocality("ANY") * 100).round.toInt shouldBe 53
      }
    }
  }

  feature("GetCoreUsageByJobGroup") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / SparklintLogProcessor.name
      val jobSink = system.actorSelection(logProcessorPath / name)
      jobSink ! GetCoreUsageByJobGroup(41662, None, None)
      expectMsgPF() {
        case CoreUsageResponse(byJobGroup) =>
          byJobGroup shouldBe SortedMap(
            1466087848562L -> UsageByGroup(Map("myJobGroup" -> 4), 0),
            1466087890224L -> UsageByGroup(Map("myJobGroup" -> 4), 0),
            1466087931886L -> UsageByGroup(Map("myJobGroup" -> 3), 1),
            1466087973548L -> UsageByGroup(Map("myJobGroup" -> 1), 3),
            1466088015210L -> UsageByGroup(Map("myJobGroup" -> 1), 3),
            1466088056872L -> UsageByGroup(Map(), 0)
          )
      }
    }
  }

  feature("GetCoreUtilizationByJobGroup") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / SparklintLogProcessor.name
      val jobSink = system.actorSelection(logProcessorPath / name)
      jobSink ! GetCoreUtilizationByJobGroup(None, None)
      expectMsgPF() {
        case CoreUtilizationResponse(usageByJobGroup) =>
          usageByJobGroup.size shouldBe 1
          (usageByJobGroup("myJobGroup") * 100).round.toInt shouldBe 60
      }
    }
  }
}
