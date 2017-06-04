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

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{DefaultTimeout, TestKitBase}
import org.apache.spark.scheduler.TaskLocality
import org.scalatest.{FeatureSpec, Matchers}

import scala.collection.SortedMap

/**
  * @author rxue
  * @since 6/4/17.
  */
class JobSinkTest extends FeatureSpec
  with ScalatestRouteTest
  with TestKitBase
  with DefaultTimeout
  with FileBasedSparkLogTest
  with Matchers {

  import JobSink._

  feature("GetCoreUsageByLocality") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / s"$uuid-${SparklintLogProcessor.name}"
      val jobSink = system.actorSelection(logProcessorPath / s"$uuid-$name")
      jobSink ! GetCoreUsageByLocality(5, None, None, testActor)
      expectMsg(CoreUsageByLocalityResponse(Map(
        TaskLocality.PROCESS_LOCAL -> SortedMap(
          1466087848562L -> 0, 1466087890224L -> 0, 1466087931886L -> 0, 1466087973548L -> 0, 1466088015210L -> 0, 1466088056872L -> 0
        ),
        TaskLocality.NODE_LOCAL -> SortedMap(
          1466087848562L -> 0, 1466087890224L -> 0, 1466087931886L -> 0, 1466087973548L -> 0, 1466088015210L -> 0, 1466088056872L -> 0
        ),
        TaskLocality.RACK_LOCAL -> SortedMap(
          1466087848562L -> 1, 1466087890224L -> 0, 1466087931886L -> 0, 1466087973548L -> 0, 1466088015210L -> 0, 1466088056872L -> 0
        ),
        TaskLocality.NO_PREF -> SortedMap.empty,
        TaskLocality.ANY -> SortedMap(
          1466087848562L -> 3, 1466087890224L -> 4, 1466087931886L -> 2, 1466087973548L -> 1, 1466088015210L -> 0, 1466088056872L -> 0
        )
      ), SortedMap(
        1466087848562L -> 4, 1466087890224L -> 4, 1466087931886L -> 4, 1466087973548L -> 4, 1466088015210L -> 4, 1466088056872L -> 0
      )))
    }
  }

  feature("GetCoreUtilizationByLocality") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / s"$uuid-${SparklintLogProcessor.name}"
      val jobSink = system.actorSelection(logProcessorPath / s"$uuid-$name")
      jobSink ! GetCoreUtilizationByLocality(None, None, testActor)
      expectMsgPF() {
        case CoreUtilizationByLocalityResponse(usageByLocality) =>
          (usageByLocality(TaskLocality.PROCESS_LOCAL) * 100).round.toInt shouldBe 0
          (usageByLocality(TaskLocality.NODE_LOCAL) * 100).round.toInt shouldBe 1
          (usageByLocality(TaskLocality.RACK_LOCAL) * 100).round.toInt shouldBe 6
          (usageByLocality(TaskLocality.NO_PREF) * 100).round.toInt shouldBe 0
          (usageByLocality(TaskLocality.ANY) * 100).round.toInt shouldBe 53
      }
    }
  }

  feature("GetCoreUsageByJobGroup") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / s"$uuid-${SparklintLogProcessor.name}"
      val jobSink = system.actorSelection(logProcessorPath / s"$uuid-$name")
      jobSink ! GetCoreUsageByJobGroup(5, None, None, testActor)
      expectMsgPF() {
        case CoreUsageByJobGroupResponse(byJobGroup, availableCores) =>
          byJobGroup shouldBe Map("myJobGroup" -> SortedMap(
            1466087848562L -> 4, 1466087890224L -> 4, 1466087931886L -> 3, 1466087973548L -> 1, 1466088015210L -> 1, 1466088056872L -> 0
          ))
          availableCores shouldBe SortedMap(
            1466087848562L -> 4, 1466087890224L -> 4, 1466087931886L -> 4, 1466087973548L -> 4, 1466088015210L -> 4, 1466088056872L -> 0
          )
      }
    }
  }

  feature("GetCoreUtilizationByJobGroup") {
    scenario("normally") {
      val reader = readSampleLog()
      val logProcessorPath = reader.path / s"$uuid-${SparklintLogProcessor.name}"
      val jobSink = system.actorSelection(logProcessorPath / s"$uuid-$name")
      jobSink ! GetCoreUtilizationByJobGroup(None, None, testActor)
      expectMsgPF() {
        case CoreUtilizationByJobGroupResponse(usageByJobGroup) =>
          usageByJobGroup.size shouldBe 1
          (usageByJobGroup("myJobGroup") * 100).round.toInt shouldBe 60
      }
    }
  }
}
