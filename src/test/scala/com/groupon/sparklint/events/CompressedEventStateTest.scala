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
package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.TestUtils
import com.groupon.sparklint.data.StageIdentifier
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.scheduler.TaskLocality._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/13/16.
  */
class CompressedEventStateTest extends FlatSpec with Matchers {
  it should "accumulate core usage correctly" in {
    val evState = new CompressedEventState()
    val file = new File(TestUtils.resource("spark_event_log_example"))
    val eventSource = FileEventSource(file, evState)

    TestUtils.replay(eventSource)
    val state = evState.state
    val coreUsage = state.coreUsage
    coreUsage.size shouldBe 5
    // should be when first task was submitted
    val firstTaskTime = 1466087848562L
    coreUsage(TaskLocality.ANY).origin shouldBe firstTaskTime
    coreUsage(TaskLocality.PROCESS_LOCAL).origin shouldBe firstTaskTime
    coreUsage(TaskLocality.NODE_LOCAL).origin shouldBe firstTaskTime
    coreUsage(TaskLocality.RACK_LOCAL).origin shouldBe firstTaskTime
    coreUsage(TaskLocality.NO_PREF).origin shouldBe firstTaskTime

    coreUsage(TaskLocality.ANY).storage.sum shouldBe 442010
    coreUsage(TaskLocality.PROCESS_LOCAL).storage.sum shouldBe 926
    coreUsage(TaskLocality.NODE_LOCAL).storage.sum shouldBe 11338
    coreUsage(TaskLocality.RACK_LOCAL).storage.sum shouldBe 49480
    coreUsage(TaskLocality.NO_PREF).storage.sum shouldBe 0

    // the core usage stats' time resolution should be 500 ms
    coreUsage(TaskLocality.ANY).resolution shouldBe 500
    coreUsage(TaskLocality.PROCESS_LOCAL).resolution shouldBe 500
    coreUsage(TaskLocality.NODE_LOCAL).resolution shouldBe 100 // less record than other locality
    coreUsage(TaskLocality.RACK_LOCAL).resolution shouldBe 500
    coreUsage(TaskLocality.NO_PREF).resolution shouldBe 1 // no record
  }

  it should "accumulate stage metrics correctly" in {
    val evState = new CompressedEventState()
    val file = new File(TestUtils.resource("spark_event_log_example"))
    val eventSource = FileEventSource(file, evState)
    TestUtils.replay(eventSource)
    val state = evState.state
    val stageMetrics = state.stageMetrics

    stageMetrics.size shouldBe 1
    val taskMetricsByLocality = stageMetrics(StageIdentifier('myJobGroup, 'myJobDescription, "count at <console>:22"))
    taskMetricsByLocality.metricsRepo.size shouldBe 4
    taskMetricsByLocality.metricsRepo should contain key PROCESS_LOCAL -> 'ResultTask
    taskMetricsByLocality.metricsRepo should contain key RACK_LOCAL -> 'ResultTask
    taskMetricsByLocality.metricsRepo should contain key ANY -> 'ResultTask
    taskMetricsByLocality.metricsRepo should contain key NODE_LOCAL -> 'ResultTask
  }

  it should "undo events correctly" in {
    val evState1 = new CompressedEventState(30)
    val eventSource1 = FileEventSource(new File(TestUtils.resource("spark_event_log_example")), evState1)
    eventSource1.forwardEvents(300)
    val expected = evState1.state
    val evState2 = new CompressedEventState(30)
    val eventSource2 = FileEventSource(new File(TestUtils.resource("spark_event_log_example")), evState2)
    eventSource2.forwardEvents(350)
    eventSource2.rewindEvents(50)
    val actual = evState2.state
    actual.coreUsage.size shouldBe expected.coreUsage.size
    actual.coreUsage.foreach({
      // The resolution can be different but the sum should be the same
      case (locality, sink) => sink.storage.sum shouldBe expected.coreUsage(locality).storage.sum
    })
    actual.executorInfo shouldBe expected.executorInfo
    // No further requirement on stageMetrics part because
    actual.stageMetrics.size >= expected.stageMetrics.size shouldBe true
    actual.stageIdLookup shouldBe expected.stageIdLookup
    actual.runningTasks shouldBe expected.runningTasks
    // after rewinding, the lastUpdatedAt is usually one event away
    actual.lastUpdatedAt >= expected.lastUpdatedAt shouldBe true
    actual.applicationEndedAt shouldBe expected.applicationEndedAt
    actual.firstTaskAt shouldBe expected.firstTaskAt
  }
}
