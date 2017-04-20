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

import com.groupon.sparklint.common.Utils
import org.apache.spark.scheduler._

import scala.collection.mutable

/**
  * A class that provides various information about the pointer progress of a specific EventSource through
  * the event receiver interface.
  *
  * @author swhitear, rxue
  * @since 9/12/16.
  */
@throws[IllegalArgumentException]
class EventProgressTracker(val eventProgress: EventProgress = EventProgress.empty(),
                           val taskProgress: EventProgress = EventProgress.empty(),
                           val stageProgress: EventProgress = EventProgress.empty(),
                           val jobProgress: EventProgress = EventProgress.empty())
  extends EventProgressTrackerLike with EventReceiverLike {

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
    taskProgress.active += taskNameFromInfo(event.taskInfo)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskProgress.complete += 1
    taskProgress.active -= taskNameFromInfo(event.taskInfo)
  }

  override def unTaskStart(event: SparkListenerTaskStart): Unit = {
    taskProgress.started -= 1
    taskProgress.active -= taskNameFromInfo(event.taskInfo)
  }

  override def unTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskProgress.complete -= 1
    taskProgress.active += taskNameFromInfo(event.taskInfo)
  }

  private def taskNameFromInfo(taskInf: TaskInfo) =
    s"ID${taskInf.taskId}:${taskInf.taskLocality}:${taskInf.host}(attempt ${taskInf.attemptNumber})"

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    stageProgress.started += 1
    stageProgress.active += event.stageInfo.name
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    stageProgress.complete += 1
    stageProgress.active -= event.stageInfo.name
  }

  override def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    stageProgress.started -= 1
    stageProgress.active -= event.stageInfo.name
  }

  override def unStageCompleted(event: SparkListenerStageCompleted): Unit = {
    stageProgress.complete -= 1
    stageProgress.active += event.stageInfo.name
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    jobProgress.started += 1
    jobProgress.active += jobNameFromInfo(event)
  }

  private def jobNameFromInfo(event: SparkListenerJobStart): String = {
    val props = event.properties
    val propertyExtract = s"${props.getProperty("spark.job.description", Utils.UNKNOWN_STRING)}"
    val name = s"ID${event.jobId}:$propertyExtract"
    jobTracker.getOrElseUpdate(event.jobId, name)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    jobProgress.complete += 1
    jobNameFromId(event.jobId).foreach(jobName => jobProgress.active -= jobName)
  }

  private def jobNameFromId(id: Int): Option[String] = {
    jobTracker.get(id)
  }

  override def unJobStart(event: SparkListenerJobStart): Unit = {
    jobProgress.started -= 1
    jobNameFromId(event.jobId).foreach(jobName => jobProgress.active -= jobName)
  }

  override def unJobEnd(event: SparkListenerJobEnd): Unit = {
    jobProgress.complete -= 1
    jobNameFromId(event.jobId).foreach(jobName => jobProgress.active += jobName)
  }

  override def toString: String =
    s"Event: $eventProgress, Task: $taskProgress, Stage: $stageProgress, Job: $jobProgress"
}






