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

import com.groupon.sparklint.TestUtils
import com.groupon.sparklint.data._
import com.groupon.sparklint.events._
import org.apache.spark.scheduler.TaskLocality
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author rxue
  * @since 8/19/16.
  */
class SparklintStateAnalyzerTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  var eventSource: FileEventSource      = _
  var eventState : CompressedEventState = _

  override protected def beforeEach(): Unit = {
    eventState = new CompressedEventState()
    val file = new File(TestUtils.resource("spark_event_log_example"))
    eventSource = FileEventSource(file, eventState)
  }

  it should "getTimeUntilFirstTask correctly" in {
    val appStart = 1466087746466L
    val firstTaskSubmitted = 1466087848562L
    TestUtils.replay(eventSource)
    SparklintStateAnalyzer(eventSource).getTimeUntilFirstTask shouldBe Some(firstTaskSubmitted - appStart)
  }

  it should "getCumulativeCoreUsage correctly" in {
    TestUtils.replay(eventSource)
    SparklintStateAnalyzer(eventSource).getCumulativeCoreUsage shouldBe Some(Map(0 -> 67500, 1 -> 3873L, 2 -> 13938L, 3 -> 20500L, 4 -> 102500L))
  }

  it should "getIdleTime correctly" in {
    TestUtils.replay(eventSource)
    SparklintStateAnalyzer(eventSource).getIdleTime shouldBe Some(169596L)
  }

  it should "getIdleTimeSinceFirstTask correctly" in {
    TestUtils.replay(eventSource)
    SparklintStateAnalyzer(eventSource).getIdleTimeSinceFirstTask shouldBe Some(67500L)
  }

  it should "getMaxConcurrentTasks correctly" in {
    TestUtils.replay(eventSource)
    SparklintStateAnalyzer(eventSource).getMaxConcurrentTasks shouldBe Some(4)
  }

  it should "getMaxAllocatedCores correctly" in {
    TestUtils.replay(eventSource)
    SparklintStateAnalyzer(eventSource).getMaxAllocatedCores shouldBe Some(6)
  }

  it should "getRunningTasks correctly" in {
    // Starts with 0
    SparklintStateAnalyzer(eventSource).getRunningTasks shouldBe Some(0)
    TestUtils.replay(eventSource, count = 95)
    // Accumulate to 4 during run
    SparklintStateAnalyzer(eventSource).getRunningTasks shouldBe Some(4)
  }

  it should "getCurrentTaskByExecutors correctly" in {
    // Starts with 0
    SparklintStateAnalyzer(eventSource).getCurrentTaskByExecutors shouldBe None
    TestUtils.replay(eventSource, count = 95)

    // Accumulate to 4 during run
    SparklintStateAnalyzer(eventSource).getCurrentTaskByExecutors.get shouldEqual
      Map("1" -> List(
        SparklintTaskInfo(46, "1", 45, 0, 1466087882535L, "ANY", false),
        SparklintTaskInfo(41, "1", 39, 0, 1466087875648L, "ANY", false)
      ), "2" -> List(
        SparklintTaskInfo(42, "2", 40, 0, 1466087875869L, "ANY", false),
        SparklintTaskInfo(43, "2", 41, 0, 1466087877653L, "ANY", false)
      ))
  }

  it should "getLocalityStatsByStageIdentifier correctly if stage identifier hit" in {
    TestUtils.replay(eventSource)
    val actual: SparklintStageMetrics = SparklintStateAnalyzer(eventSource)
      .getLocalityStatsByStageIdentifier(StageIdentifier('myJobGroup, 'myJobDescription, "count at <console>:22")).get
    actual.metricsRepo.size shouldBe 4
    actual.metricsRepo should contain key (TaskLocality.PROCESS_LOCAL -> 'ResultTask)
    actual.metricsRepo should contain key (TaskLocality.RACK_LOCAL -> 'ResultTask)
    actual.metricsRepo should contain key (TaskLocality.ANY -> 'ResultTask)
    actual.metricsRepo should contain key (TaskLocality.NODE_LOCAL -> 'ResultTask)
  }
}
