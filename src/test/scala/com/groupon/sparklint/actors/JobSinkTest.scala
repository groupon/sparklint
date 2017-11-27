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
      jobSink ! GetCoreUsageByLocality(40000, None, None)
      expectMsg(CoreUsageResponse(
        SortedMap(
          1466087848562L -> UsageByGroup(Map("RACK_LOCAL" -> 0.8979896940008907, "ANY" -> 2.3934728672307397, "NODE_LOCAL" -> 0.23038997391691585), 0.4781474648514541),
          1466087880000L -> UsageByGroup(Map("ANY" -> 4.000375), 0.0),
          1466087920000L -> UsageByGroup(Map("ANY" -> 2.01115, "RACK_LOCAL" -> 0.372125, "NODE_LOCAL" -> 0.102375), 1.5143499999999999),
          1466087960000L -> UsageByGroup(Map("ANY" -> 2.670275), 1.3297249999999998),
          1466088000000L -> UsageByGroup(Map(), 4.0),
          1466088040000L -> UsageByGroup(Map("ANY" -> 0.9919306634787807, "RACK_LOCAL" -> 0.4754931261207412, "PROCESS_LOCAL" -> 0.03182904961147639), 2.5007471607890017),
          1466088053384L -> UsageByGroup(Map(), 0.0)
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
          (usageByLocality("ANY") * 100).round.toInt shouldBe 54
      }
    }
  }

  feature("GetCoreUsageByJobGroup") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / SparklintLogProcessor.name
      val jobSink = system.actorSelection(logProcessorPath / name)
      jobSink ! GetCoreUsageByJobGroup(40000, None, None)
      expectMsgPF() {
        case CoreUsageResponse(byJobGroup) =>
          byJobGroup shouldBe SortedMap(
            1466087848562L -> UsageByGroup(Map("myJobGroup" -> 3.5218525351485463), 0.47814746485145365),
            1466087880000L -> UsageByGroup(Map("myJobGroup" -> 4.000375), 0),
            1466087920000L -> UsageByGroup(Map("myJobGroup" -> 2.48565), 1.5143499999999999),
            1466087960000L -> UsageByGroup(Map("myJobGroup" -> 2.670275), 1.3297249999999998),
            1466088000000L -> UsageByGroup(Map(), 4.0),
            1466088040000L -> UsageByGroup(Map("myJobGroup" -> 1.4992528392109983), 2.5007471607890017),
            1466088053384L -> UsageByGroup(Map(), 0.0)
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
          (usageByJobGroup("myJobGroup") * 100).round.toInt shouldBe 61
      }
    }
  }
}
