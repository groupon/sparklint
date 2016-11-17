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

import com.groupon.sparklint.TestUtils._
import com.groupon.sparklint.common.Utils
import org.apache.spark.scheduler.TaskLocality
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class EventSourceProgressTest extends FlatSpec with Matchers {

  it should "default to empty progress trackers" in {
    val progress = EventSourceProgress()

    testProgress(progress.eventProgress, 0)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 0)
  }

  it should "preprocess as expected" in {
    val progress = EventSourceProgress()

    progress.preprocess(sparkAppStart())
    testProgress(progress.eventProgress, 1)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 0)

    progress.preprocess(sparkTaskStartEventFromId(0))
    testProgress(progress.eventProgress, 2)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 0)

    progress.preprocess(sparkTaskEndEventFromId(0))
    testProgress(progress.eventProgress, 3)
    testProgress(progress.taskProgress, 1)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 0)

    progress.preprocess(sparkStageSubmittedFromId(0))
    testProgress(progress.eventProgress, 4)
    testProgress(progress.taskProgress, 1)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 0)

    progress.preprocess(sparkStageCompletedFromId(0))
    testProgress(progress.eventProgress, 5)
    testProgress(progress.taskProgress, 1)
    testProgress(progress.stageProgress, 1)
    testProgress(progress.jobProgress, 0)

    progress.preprocess(sparkJobStartFromId(0))
    testProgress(progress.eventProgress, 6)
    testProgress(progress.taskProgress, 1)
    testProgress(progress.stageProgress, 1)
    testProgress(progress.jobProgress, 0)

    progress.preprocess(sparkJobEndFromId(0))
    testProgress(progress.eventProgress, 7)
    testProgress(progress.taskProgress, 1)
    testProgress(progress.stageProgress, 1)
    testProgress(progress.jobProgress, 1)
  }

  it should "process on events as expected" in {
    val progress = EventSourceProgress()

    progress.onEvent(sparkAppStart())
    testProgress(progress.eventProgress, 1, 1)
    testProgress(progress.taskProgress, 0, 0)
    testProgress(progress.stageProgress, 0, 0)
    testProgress(progress.jobProgress, 0, 0)

    progress.onEvent(sparkJobStartFromId(0))
    testProgress(progress.eventProgress, 2, 2)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    val stageInfo = sparkStageInfo(stageId = 0, name = "test-stage-name")
    progress.onEvent(sparkStageSubmitted(stageInfo))
    testProgress(progress.eventProgress, 3, 3)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    val taskInfo1 = sparkTaskInfo(taskId = 1, host = "host1")
    val taskInfo2 = sparkTaskInfo(taskId = 2, host = "host2")
    progress.onEvent(sparkTaskStartEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    progress.onEvent(sparkTaskStartEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progress.eventProgress, 5, 5)
    testProgress(progress.taskProgress, 2, 0, taskName(1, "host1"), taskName(2, "host2"))
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.onEvent(sparkTaskEndEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    testProgress(progress.eventProgress, 6, 6)
    testProgress(progress.taskProgress, 2, 1, taskName(2, "host2"))
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.onEvent(sparkTaskEndEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progress.eventProgress, 7, 7)
    testProgress(progress.taskProgress, 2, 2)
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.onEvent(sparkStageCompleted(stageInfo))
    testProgress(progress.eventProgress, 8, 8)
    testProgress(progress.taskProgress, 2, 2)
    testProgress(progress.stageProgress, 1, 1)
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.onEvent(sparkJobEndFromId(0))
    testProgress(progress.eventProgress, 9, 9)
    testProgress(progress.taskProgress, 2, 2)
    testProgress(progress.stageProgress, 1, 1)
    testProgress(progress.jobProgress, 1, 1)
  }

  it should "process un events as expected" in {
    val progress = EventSourceProgress()

    progress.onEvent(sparkAppStart())
    progress.onEvent(sparkJobStartFromId(0))
    val stageInfo = sparkStageInfo(stageId = 0, name = "test-stage-name")
    progress.onEvent(sparkStageSubmitted(stageInfo))
    val taskInfo1 = sparkTaskInfo(taskId = 1, host = "host1")
    val taskInfo2 = sparkTaskInfo(taskId = 2, host = "host2")
    progress.onEvent(sparkTaskStartEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    progress.onEvent(sparkTaskStartEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    progress.onEvent(sparkTaskEndEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    progress.onEvent(sparkTaskEndEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    progress.onEvent(sparkStageCompleted(stageInfo))
    progress.onEvent(sparkJobEndFromId(0))
    testProgress(progress.eventProgress, 9, 9)
    testProgress(progress.taskProgress, 2, 2)
    testProgress(progress.stageProgress, 1, 1)
    testProgress(progress.jobProgress, 1, 1)

    progress.unEvent(sparkJobEndFromId(0))
    testProgress(progress.eventProgress, 8, 8)
    testProgress(progress.taskProgress, 2, 2)
    testProgress(progress.stageProgress, 1, 1)
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkStageCompleted(stageInfo))
    testProgress(progress.eventProgress, 7, 7)
    testProgress(progress.taskProgress, 2, 2)
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkTaskEndEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progress.eventProgress, 6, 6)
    testProgress(progress.taskProgress, 2, 1, taskName(2, "host2"))
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkTaskEndEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    testProgress(progress.eventProgress, 5, 5)
    testProgress(progress.taskProgress, 2, 0, taskName(1, "host1"), taskName(2, "host2"))
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkTaskStartEvent(taskId = taskInfo2.taskId)(taskInfo = taskInfo2))
    testProgress(progress.eventProgress, 4, 4)
    testProgress(progress.taskProgress, 1, 0, taskName(1, "host1"))
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkTaskStartEvent(taskId = taskInfo1.taskId)(taskInfo = taskInfo1))
    testProgress(progress.eventProgress, 3, 3)
    testProgress(progress.taskProgress, 0, 0)
    testProgress(progress.stageProgress, 1, 0, "test-stage-name")
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkStageSubmitted(stageInfo))
    testProgress(progress.eventProgress, 2, 2)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 1, 0, jobName(0))

    progress.unEvent(sparkJobStartFromId(0))
    testProgress(progress.eventProgress, 1, 1)
    testProgress(progress.taskProgress, 0, 0)
    testProgress(progress.stageProgress, 0, 0)
    testProgress(progress.jobProgress, 0, 0)

    progress.unEvent(sparkAppStart())
    testProgress(progress.eventProgress, 0)
    testProgress(progress.taskProgress, 0)
    testProgress(progress.stageProgress, 0)
    testProgress(progress.jobProgress, 0)
  }


  private def testProgress(progress: EventProgressLike, count: Int,
                           started: Int, complete: Int, active: String*): Unit = {
    progress.count shouldEqual count
    progress.started shouldEqual started
    progress.complete shouldEqual complete
    progress.active shouldEqual active.toSet
  }

  private def testProgress(progress: EventProgressLike, count: Int): Unit = {
    testProgress(progress, count, 0, 0)
  }

  private def testProgress(progress: EventProgressLike, started: Int, complete: Int, active: String*): Unit = {
    testProgress(progress, 0, started, complete, active:_*)
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
    val progress = EventProgress(10, 6, 4, Set("test-in-flight1", "test-in-flight2"))

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
    val progress = EventProgress(10, 10, 10, Set())

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