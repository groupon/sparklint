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

import com.groupon.sparklint.common.Utils
import org.apache.spark.scheduler._

import scala.collection.mutable

/**
  * A case class that provides various information about the pointer progress of a specific EventSource through
  * the event receiver interface.
  *
  * @author swhitear 
  * @since 9/12/16.
  */
@throws[IllegalArgumentException]
class EventProgressTracker(val eventProgress: EventProgress = EventProgress.empty(),
                           val taskProgress: EventProgress = EventProgress.empty(),
                           val stageProgress: EventProgress = EventProgress.empty(),
                           val  jobProgress: EventProgress = EventProgress.empty())
  extends EventProgressTrackerLike with EventReceiverLike {

  require(eventProgress != null)
  require(taskProgress != null)
  require(stageProgress != null)
  require(jobProgress != null)

  private val jobTracker = mutable.Map.empty[Int, String]

  override def onPreprocEvent(event: SparkListenerEvent): Unit = {
    eventProgress.count += 1
  }

  override def preprocTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskProgress.count += 1
  }

  override def preprocStageCompleted(event: SparkListenerStageCompleted): Unit = {
    stageProgress.count += 1
  }

  override def preprocJobEnd(event: SparkListenerJobEnd): Unit = {
    jobProgress.count += 1
  }

  override def onOnEvent(event: SparkListenerEvent): Unit = {
    eventProgress.started += 1
    eventProgress.complete += 1
  }

  override def onUnEvent(event: SparkListenerEvent): Unit = {
    eventProgress.started -= 1
    eventProgress.complete -= 1
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    taskProgress.started += 1
    taskProgress.active = taskProgress.active + taskNameFromInfo(event.taskInfo)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskProgress.complete += 1
    taskProgress.active = taskProgress.active - taskNameFromInfo(event.taskInfo)
  }

  override def unTaskStart(event: SparkListenerTaskStart): Unit = {
    taskProgress.started -= 1
    taskProgress.active = taskProgress.active - taskNameFromInfo(event.taskInfo)
  }

  override def unTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskProgress.complete -= 1
    taskProgress.active = taskProgress.active + taskNameFromInfo(event.taskInfo)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    stageProgress.started += 1
    stageProgress.active = stageProgress.active + event.stageInfo.name
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    stageProgress.complete += 1
    stageProgress.active = stageProgress.active - event.stageInfo.name
  }

  override def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    stageProgress.started -= 1
    stageProgress.active = stageProgress.active - event.stageInfo.name
  }

  override def unStageCompleted(event: SparkListenerStageCompleted): Unit = {
    stageProgress.complete -= 1
    stageProgress.active = taskProgress.active + event.stageInfo.name
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    jobProgress.started += 1
    jobProgress.active = jobProgress.active + jobNameFromInfo(event)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    jobProgress.complete += 1
    jobProgress.active = jobProgress.active - jobNameFromId(event.jobId)
  }

  override def unJobStart(event: SparkListenerJobStart): Unit = {
    jobProgress.started -= 1
    jobProgress.active = jobProgress.active - jobNameFromInfo(event)
  }

  override def unJobEnd(event: SparkListenerJobEnd): Unit = {
    jobProgress.complete -= 1
    jobProgress.active = jobProgress.active + jobNameFromId(event.jobId)
  }

  override def toString: String =
    s"Event: $eventProgress, Task: $taskProgress, Stage: $stageProgress, Job: $jobProgress"

  private def taskNameFromInfo(taskInf: TaskInfo) =
    s"ID${taskInf.taskId}:${taskInf.taskLocality}:${taskInf.host}:ATT${taskInf.attemptNumber}"

  private def jobNameFromInfo(event: SparkListenerJobStart): String = {
    val props = event.properties
    val propertyExtract = s"${props.getProperty("spark.job.description", Utils.UNKNOWN_STRING)}"
    val name = s"ID${event.jobId}:$propertyExtract"
    jobTracker.getOrElseUpdate(event.jobId, name)
  }

  private def jobNameFromId(id: Int): String = {
    jobTracker(id)
  }
}

trait EventProgressTrackerLike {
  val eventProgress: EventProgress
  val taskProgress : EventProgress
  val stageProgress: EventProgress
  val jobProgress  : EventProgress
}

class EventProgress(var count: Int, var started: Int, var complete: Int, var active: Set[String]) {
  require(count >= 0)
  require(started >= 0 && started <= count)
  require(complete >= 0 && complete <= count)
  require(complete <= started)
  require(active != null)

  def safeCount: Double = if (count == 0) 1 else count

  def percent = ((complete / safeCount) * 100).round.toInt

  def hasNext = complete < count

  def hasPrevious = complete > 0

  def inFlightCount = started - complete

  override def toString: String = s"$complete of $count with $inFlightCount active"
}

object EventProgress {
  def empty(): EventProgress = new EventProgress(0, 0, 0, Set.empty)
}
