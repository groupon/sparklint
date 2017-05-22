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

import java.io.File

import com.groupon.sparklint.common.TestUtils._
import com.groupon.sparklint.data.SparklintStageIdentifier
import org.apache.spark.scheduler.TaskLocality._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/13/16.
  */
class CompressedStateManagerTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  var eventSource: FreeScrollEventSource = _
  var eventState: CompressedStateManager = _
  var file: File = _

  override protected def beforeEach(): Unit = {
    file = new File(resource("spark_event_log_example"))
    eventSource = EventSource.fromFile(file, compressStorage = true)
  }

  it should "accumulate core usage correctly" in {
    replay(eventSource)
    val state = eventSource.appState
    val coreUsage = state.coreUsageByLocality
    coreUsage.size shouldBe 5
    // should be when first task was submitted
    val firstTaskTime = 1466087848562L
    coreUsage(ANY).origin shouldBe firstTaskTime
    coreUsage(PROCESS_LOCAL).origin shouldBe firstTaskTime
    coreUsage(NODE_LOCAL).origin shouldBe firstTaskTime
    coreUsage(RACK_LOCAL).origin shouldBe firstTaskTime
    coreUsage(NO_PREF).origin shouldBe firstTaskTime

    coreUsage(ANY).storage.sum shouldBe 442010
    coreUsage(PROCESS_LOCAL).storage.sum shouldBe 926
    coreUsage(NODE_LOCAL).storage.sum shouldBe 11338
    coreUsage(RACK_LOCAL).storage.sum shouldBe 49480
    coreUsage(NO_PREF).storage.sum shouldBe 0

    // the core usage stats' time resolution should be 500 ms
    coreUsage(ANY).resolution shouldBe 500
    coreUsage(PROCESS_LOCAL).resolution shouldBe 500
    coreUsage(NODE_LOCAL).resolution shouldBe 100 // less record than other locality
    coreUsage(RACK_LOCAL).resolution shouldBe 500
    coreUsage(NO_PREF).resolution shouldBe 1 // no record
  }

  it should "accumulate stage metrics correctly" in {
    replay(eventSource)
    val state = eventSource.appState
    val stageMetrics = state.stageMetrics

    stageMetrics.size shouldBe 2
    val taskMetricsByLocality = stageMetrics(SparklintStageIdentifier('myJobGroup, 'myJobDescription, "count at <console>:22", 'default))
    taskMetricsByLocality.metricsRepo.size shouldBe 4
    taskMetricsByLocality.metricsRepo should contain key PROCESS_LOCAL -> 'ResultTask
    taskMetricsByLocality.metricsRepo should contain key RACK_LOCAL -> 'ResultTask
    taskMetricsByLocality.metricsRepo should contain key ANY -> 'ResultTask
    taskMetricsByLocality.metricsRepo should contain key NODE_LOCAL -> 'ResultTask
    val taskMetricsForMyPool = stageMetrics(SparklintStageIdentifier('myJobGroup, 'myJobDescription, "count at <console>:22", 'myPool))
    taskMetricsForMyPool.metricsRepo.size shouldBe 3
    taskMetricsForMyPool.metricsRepo should contain key RACK_LOCAL -> 'ResultTask
    taskMetricsForMyPool.metricsRepo should contain key ANY -> 'ResultTask
    taskMetricsForMyPool.metricsRepo should contain key NODE_LOCAL -> 'ResultTask

  }

  it should "undo events correctly" in {
    eventSource.forwardEvents(300)

    val eventSource2 = EventSource.fromFile(file, compressStorage = true)

    val expected = eventSource.appState
    eventSource2.forwardEvents(350)
    eventSource2.rewindEvents(50)
    val actual = eventSource.appState
    actual.coreUsageByLocality.size shouldBe expected.coreUsageByLocality.size
    actual.coreUsageByLocality.foreach({
      // The resolution can be different but the sum should be the same
      case (locality, sink) => sink.storage.sum shouldBe expected.coreUsageByLocality(locality).storage.sum
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
