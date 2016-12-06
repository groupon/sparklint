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

import java.io.File

import com.groupon.sparklint.common.TestUtils
import com.groupon.sparklint.data._
import com.groupon.sparklint.events.{CompressedStateManager, EventSourceMeta, FileEventSource}
import org.apache.spark.scheduler.TaskLocality._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author rxue
  * @since 8/19/16.
  */
class SparklintStateAnalyzerTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  var eventSource : FileEventSource        = _
  var meta        : EventSourceMeta        = _
  var stateManager: CompressedStateManager = _

  override protected def beforeEach(): Unit = {
    stateManager = new CompressedStateManager()
    meta = new EventSourceMeta()

    val file = new File(TestUtils.resource("spark_event_log_example"))
    eventSource = FileEventSource(file, Seq(meta, stateManager))
  }

  it should "getTimeUntilFirstTask correctly" in {
    val appStart = 1466087746466L
    val firstTaskSubmitted = 1466087848562L
    TestUtils.replay(eventSource)

    val time = new SparklintStateAnalyzer(meta, stateManager).getTimeUntilFirstTask
    time shouldBe Some(firstTaskSubmitted - appStart)
  }

  it should "getCumulativeCoreUsage correctly" in {
    TestUtils.replay(eventSource)
    new SparklintStateAnalyzer(meta, stateManager).getCumulativeCoreUsage shouldBe
      Some(Map(0 -> 67500, 1 -> 3873L, 2 -> 13938L, 3 -> 20500L, 4 -> 102500L))
  }

  it should "getIdleTime correctly" in {
    TestUtils.replay(eventSource)
    new SparklintStateAnalyzer(meta, stateManager).getIdleTime shouldBe Some(169596L)
  }

  it should "getIdleTimeSinceFirstTask correctly" in {
    TestUtils.replay(eventSource)
    new SparklintStateAnalyzer(meta, stateManager).getIdleTimeSinceFirstTask shouldBe Some(67500L)
  }

  it should "getMaxConcurrentTasks correctly" in {
    TestUtils.replay(eventSource)
    new SparklintStateAnalyzer(meta, stateManager).getMaxConcurrentTasks shouldBe Some(4)
  }

  it should "getMaxAllocatedCores correctly" in {
    TestUtils.replay(eventSource)
    new SparklintStateAnalyzer(meta, stateManager).getMaxAllocatedCores shouldBe Some(6)
  }

  it should "getRunningTasks correctly" in {
    // Starts with 0
    new SparklintStateAnalyzer(meta, stateManager).getRunningTasks shouldBe Some(0)
    TestUtils.replay(eventSource, count = 98)
    // Accumulate to 4 during run
    new SparklintStateAnalyzer(meta, stateManager).getRunningTasks shouldBe Some(4)
  }

  it should "getCurrentTaskByExecutors correctly" in {
    // Starts with 0
    new SparklintStateAnalyzer(meta, stateManager).getCurrentTaskByExecutors shouldBe None
    TestUtils.replay(eventSource, count = 100)

    // Accumulate to 4 during run
    new SparklintStateAnalyzer(meta, stateManager).getCurrentTaskByExecutors.get shouldEqual
      Map("1" -> List(
        SparklintTaskInfo(46, "1", 45, 0, 1466087882535L, 'ANY, speculative = false),
        SparklintTaskInfo(41, "1", 39, 0, 1466087875648L, 'ANY, speculative = false)
      ), "2" -> List(
        SparklintTaskInfo(42, "2", 40, 0, 1466087875869L, 'ANY, speculative = false),
        SparklintTaskInfo(43, "2", 41, 0, 1466087877653L, 'ANY, speculative = false)
      ))
  }

  it should "getLocalityStatsByStageIdentifier correctly if stage identifier hit" in {
    TestUtils.replay(eventSource)
    val actual: SparklintStageMetrics = new SparklintStateAnalyzer(meta, stateManager)
      .getLocalityStatsByStageIdentifier(SparklintStageIdentifier('myJobGroup, 'myJobDescription, "count at <console>:22")).get
    actual.metricsRepo.size shouldBe 4
    actual.metricsRepo should contain key (PROCESS_LOCAL -> 'ResultTask)
    actual.metricsRepo should contain key (RACK_LOCAL -> 'ResultTask)
    actual.metricsRepo should contain key (ANY -> 'ResultTask)
    actual.metricsRepo should contain key (NODE_LOCAL -> 'ResultTask)
  }
}
