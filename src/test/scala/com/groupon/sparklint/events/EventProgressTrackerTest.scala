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

import com.groupon.sparklint.common.TestUtils._
import com.groupon.sparklint.common.Utils
import org.apache.spark.scheduler.TaskLocality
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class EventProgressTrackerTest extends FlatSpec with Matchers {

  it should "default to empty progress trackers" in {
    val progressTracker = new EventProgressTracker()

    testProgress(progressTracker.eventProgress, 0)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 0)
  }

  it should "preprocess as expected" in {
    val progressTracker = new EventProgressTracker()

    progressTracker.preprocess(sparkAppStart())
    testProgress(progressTracker.eventProgress, 1)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 0)

    progressTracker.preprocess(sparkTaskStartEventFromId(0))
    testProgress(progressTracker.eventProgress, 2)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 0)

    progressTracker.preprocess(sparkTaskEndEventFromId(0))
    testProgress(progressTracker.eventProgress, 3)
    testProgress(progressTracker.taskProgress, 1)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 0)

    progressTracker.preprocess(sparkStageSubmittedFromId(0))
    testProgress(progressTracker.eventProgress, 4)
    testProgress(progressTracker.taskProgress, 1)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 0)

    progressTracker.preprocess(sparkStageCompletedFromId(0))
    testProgress(progressTracker.eventProgress, 5)
    testProgress(progressTracker.taskProgress, 1)
    testProgress(progressTracker.stageProgress, 1)
    testProgress(progressTracker.jobProgress, 0)

    progressTracker.preprocess(sparkJobStartFromId(0))
    testProgress(progressTracker.eventProgress, 6)
    testProgress(progressTracker.taskProgress, 1)
    testProgress(progressTracker.stageProgress, 1)
    testProgress(progressTracker.jobProgress, 0)

    progressTracker.preprocess(sparkJobEndFromId(0))
    testProgress(progressTracker.eventProgress, 7)
    testProgress(progressTracker.taskProgress, 1)
    testProgress(progressTracker.stageProgress, 1)
    testProgress(progressTracker.jobProgress, 1)
  }

  it should "process on events as expected" in {
    val progressTracker = new EventProgressTracker()

    progressTracker.onEvent(sparkAppStart())
    testProgress(progressTracker.eventProgress, 1, 1)
    testProgress(progressTracker.taskProgress, 0, 0)
    testProgress(progressTracker.stageProgress, 0, 0)
    testProgress(progressTracker.jobProgress, 0, 0)

    progressTracker.onEvent(sparkJobStartFromId(0))
    testProgress(progressTracker.eventProgress, 2, 2)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    val stageInfo = sparkStageInfo(stageId = 0, name = "test-stage-name")
    progressTracker.onEvent(sparkStageSubmitted(stageInfo))
    testProgress(progressTracker.eventProgress, 3, 3)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    val taskInfo1 = sparkTaskInfo(taskId = 1, host = "host1")
    val taskInfo2 = sparkTaskInfo(taskId = 2, host = "host2")
    progressTracker.onEvent(sparkTaskStartEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    progressTracker.onEvent(sparkTaskStartEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progressTracker.eventProgress, 5, 5)
    testProgress(progressTracker.taskProgress, 2, 0, taskName(1, "host1"), taskName(2, "host2"))
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.onEvent(sparkTaskEndEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    testProgress(progressTracker.eventProgress, 6, 6)
    testProgress(progressTracker.taskProgress, 2, 1, taskName(2, "host2"))
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.onEvent(sparkTaskEndEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progressTracker.eventProgress, 7, 7)
    testProgress(progressTracker.taskProgress, 2, 2)
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.onEvent(sparkStageCompleted(stageInfo))
    testProgress(progressTracker.eventProgress, 8, 8)
    testProgress(progressTracker.taskProgress, 2, 2)
    testProgress(progressTracker.stageProgress, 1, 1)
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.onEvent(sparkJobEndFromId(0))
    testProgress(progressTracker.eventProgress, 9, 9)
    testProgress(progressTracker.taskProgress, 2, 2)
    testProgress(progressTracker.stageProgress, 1, 1)
    testProgress(progressTracker.jobProgress, 1, 1)
  }

  it should "process un events as expected" in {
    val progressTracker = new EventProgressTracker()

    progressTracker.onEvent(sparkAppStart())
    progressTracker.onEvent(sparkJobStartFromId(0))
    val stageInfo = sparkStageInfo(stageId = 0, name = "test-stage-name")
    progressTracker.onEvent(sparkStageSubmitted(stageInfo))
    val taskInfo1 = sparkTaskInfo(taskId = 1, host = "host1")
    val taskInfo2 = sparkTaskInfo(taskId = 2, host = "host2")
    progressTracker.onEvent(sparkTaskStartEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    progressTracker.onEvent(sparkTaskStartEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    progressTracker.onEvent(sparkTaskEndEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    progressTracker.onEvent(sparkTaskEndEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    progressTracker.onEvent(sparkStageCompleted(stageInfo))
    progressTracker.onEvent(sparkJobEndFromId(0))
    testProgress(progressTracker.eventProgress, 9, 9)
    testProgress(progressTracker.taskProgress, 2, 2)
    testProgress(progressTracker.stageProgress, 1, 1)
    testProgress(progressTracker.jobProgress, 1, 1)

    progressTracker.unEvent(sparkJobEndFromId(0))
    testProgress(progressTracker.eventProgress, 8, 8)
    testProgress(progressTracker.taskProgress, 2, 2)
    testProgress(progressTracker.stageProgress, 1, 1)
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkStageCompleted(stageInfo))
    testProgress(progressTracker.eventProgress, 7, 7)
    testProgress(progressTracker.taskProgress, 2, 2)
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkTaskEndEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progressTracker.eventProgress, 6, 6)
    testProgress(progressTracker.taskProgress, 2, 1, taskName(2, "host2"))
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkTaskEndEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    testProgress(progressTracker.eventProgress, 5, 5)
    testProgress(progressTracker.taskProgress, 2, 0, taskName(1, "host1"), taskName(2, "host2"))
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkTaskStartEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progressTracker.eventProgress, 4, 4)
    testProgress(progressTracker.taskProgress, 1, 0, taskName(1, "host1"))
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkTaskStartEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    testProgress(progressTracker.eventProgress, 3, 3)
    testProgress(progressTracker.taskProgress, 0, 0)
    testProgress(progressTracker.stageProgress, 1, 0, "test-stage-name")
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkStageSubmitted(stageInfo))
    testProgress(progressTracker.eventProgress, 2, 2)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 1, 0, jobName(0))

    progressTracker.unEvent(sparkJobStartFromId(0))
    testProgress(progressTracker.eventProgress, 1, 1)
    testProgress(progressTracker.taskProgress, 0, 0)
    testProgress(progressTracker.stageProgress, 0, 0)
    testProgress(progressTracker.jobProgress, 0, 0)

    progressTracker.unEvent(sparkAppStart())
    testProgress(progressTracker.eventProgress, 0)
    testProgress(progressTracker.taskProgress, 0)
    testProgress(progressTracker.stageProgress, 0)
    testProgress(progressTracker.jobProgress, 0)
  }


  private def testProgress(progress: EventProgress, count: Int): Unit = {
    testProgress(progress, count, 0, 0)
  }

  private def testProgress(progress: EventProgress, started: Int, complete: Int, active: String*): Unit = {
    testProgress(progress, 0, started, complete, active: _*)
  }

  private def testProgress(progress: EventProgress, count: Int,
                           started: Int, complete: Int, active: String*): Unit = {
    progress.count shouldEqual count
    progress.started shouldEqual started
    progress.complete shouldEqual complete
    progress.active shouldEqual active.toSet
  }

  private def taskName(taskId: Int, host: String, attemptId: Int = 0) = {
    s"ID$taskId:${TaskLocality.NO_PREF}:$host(attempt $attemptId)"
  }

  private def jobName(jobId: Int) = {
    s"ID$jobId:${Utils.UNKNOWN_STRING}"
  }
}

class EventProgressTest extends FlatSpec with Matchers {

  it should "empty sets zero values" in {
    val progress = EventProgress.empty()

    progress.count shouldEqual 0
    progress.started shouldEqual 0
    progress.complete shouldEqual 0
    progress.inFlightCount shouldEqual 0
    progress.active shouldEqual Set.empty[String]
    progress.percent shouldEqual 0d
    progress.hasNext shouldEqual false
    progress.hasPrevious shouldEqual false
    progress.description shouldEqual "Completed 0 / 0 (0%) with 0 active."
  }

  it should "represent progress correctly" in {
    val progress = new EventProgress(10, 6, 4, mutable.Set("test-in-flight1", "test-in-flight2"))

    progress.count shouldEqual 10
    progress.started shouldEqual 6
    progress.complete shouldEqual 4
    progress.inFlightCount shouldEqual 2
    progress.active shouldEqual Set("test-in-flight1", "test-in-flight2")
    progress.percent shouldEqual 40d
    progress.hasNext shouldEqual true
    progress.hasPrevious shouldEqual true
    progress.description shouldEqual "Completed 4 / 10 (40%) with 2 active (test-in-flight1, test-in-flight2)."
  }

  it should "represent end of events correctly" in {
    val progress = new EventProgress(10, 10, 10, mutable.Set())

    progress.count shouldEqual 10
    progress.started shouldEqual 10
    progress.complete shouldEqual 10
    progress.inFlightCount shouldEqual 0
    progress.active shouldEqual Set()
    progress.percent shouldEqual 100d
    progress.hasNext shouldEqual false
    progress.hasPrevious shouldEqual true
    progress.description shouldEqual "Completed 10 / 10 (100%) with 0 active."
  }

}
