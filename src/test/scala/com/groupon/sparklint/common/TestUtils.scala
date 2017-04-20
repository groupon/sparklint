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

package com.groupon.sparklint.common

import java.util.Properties

import com.groupon.sparklint.events.FreeScrollEventSource
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.groupon.SparkPrivateMethodDelegate
import org.apache.spark.scheduler._
import org.apache.spark.storage.RDDInfo
import org.apache.spark.{Success, TaskEndReason}

import scala.collection.Map

/**
  * @author rxue
  * @since 8/19/16.
  */
object TestUtils {

  val TEST_TIME_LONG = 42l
  val TEST_NAME = "test-name"
  val TEST_APP_ID = "test-app-id"
  val TEST_EXECUTOR_ID = "test-executor"
  val TEST_HOST = "test-host"
  val TEST_TASK_TYPE = "test-task-type"
  val TEST_USER = "test-user"
  val TEST_DETAILS = "test-details"

  def resource(name: String): String = {
    ResourceHelper.convertResourcePathToFilePath(getClass.getClassLoader, name)
  }

  def replay(eventSource: FreeScrollEventSource, count: Int = Int.MaxValue): Unit = {
    eventSource.forwardEvents(count)
  }

  def sparkStageSubmittedEvent(id: Int, name: String): SparkListenerEvent = {
    SparkListenerStageSubmitted(new StageInfo(id, 42, name, 42, Seq.empty, Seq.empty, "details"))
  }

  def sparkAppStart(name: String = TEST_NAME,
                    appId: String = TEST_APP_ID,
                    time: Long = TEST_TIME_LONG,
                    user: String = TEST_USER,
                    attemptId: Option[String] = None,
                    driverLogs: Option[Map[String, String]] = None): SparkListenerApplicationStart = {
    SparkListenerApplicationStart(name, Some(appId), time, user, attemptId, driverLogs)
  }

  def sparkTaskStartEventFromId(taskId: Long): SparkListenerTaskStart = {
    sparkTaskStartEvent(taskId = taskId)()
  }

  def sparkTaskStartEvent(taskId: Long = 0,
                          stageId: Int = 0,
                          stageAttempt: Int = 0)
                         (taskInfo: TaskInfo = sparkTaskInfo(taskId)): SparkListenerTaskStart = {
    SparkListenerTaskStart(stageId, stageAttempt, taskInfo)
  }

  def sparkTaskEndEventFromId(taskId: Long): SparkListenerTaskEnd = {
    sparkTaskEndEvent(taskId = taskId)()
  }

  def sparkTaskEndEvent(taskId: Long = 0,
                        stageId: Int = 0,
                        stageAttempt: Int = 0,
                        taskType: String = TEST_TASK_TYPE,
                        taskEndReason: TaskEndReason = Success)
                       (taskInfo: TaskInfo = sparkTaskInfo(taskId),
                        taskMetrics: TaskMetrics = sparkTaskMetrics()): SparkListenerTaskEnd = {
    SparkListenerTaskEnd(stageId, stageAttempt, taskType, taskEndReason, taskInfo, taskMetrics)
  }

  def sparkTaskInfo(taskId: Long = 0,
                    index: Int = 0,
                    attempt: Int = 0,
                    launchTime: Long = TEST_TIME_LONG,
                    executorId: String = TEST_EXECUTOR_ID,
                    host: String = TEST_HOST,
                    locality: TaskLocality.TaskLocality = TaskLocality.NO_PREF): TaskInfo = {
    new TaskInfo(taskId, index, attempt, launchTime, executorId, host, locality, false)
  }

  def sparkTaskMetrics(): TaskMetrics = SparkPrivateMethodDelegate.sparkTaskMetrics()

  def sparkStageCompletedFromId(stageId: Int): SparkListenerStageCompleted = {
    sparkStageCompleted(sparkStageInfo(stageId = stageId))
  }

  def sparkStageCompleted(stageInfo: StageInfo = sparkStageInfo()): SparkListenerStageCompleted = {
    SparkListenerStageCompleted(stageInfo)
  }

  def sparkStageInfo(stageId: Int = 0,
                     attempt: Int = 0,
                     name: String = TEST_NAME,
                     numTasks: Int = 0,
                     rddInfo: Seq[RDDInfo] = Seq.empty,
                     parentIds: Seq[Int] = Seq.empty,
                     details: String = TEST_DETAILS): StageInfo = {
    new StageInfo(stageId, attempt, name, numTasks, rddInfo, parentIds, details)
  }

  def sparkStageSubmittedFromId(stageId: Int): SparkListenerStageSubmitted = {
    sparkStageSubmitted(sparkStageInfo(stageId = stageId))
  }

  def sparkStageSubmitted(stageInfo: StageInfo = sparkStageInfo(),
                          properties: Properties = new Properties()): SparkListenerStageSubmitted = {
    SparkListenerStageSubmitted(stageInfo)
  }

  def sparkJobStartFromId(jobId: Int): SparkListenerJobStart = {
    sparkJobStart(jobId = jobId)
  }

  def sparkJobStart(jobId: Int = 0,
                    time: Long = TEST_TIME_LONG,
                    stageInfos: Seq[StageInfo] = Seq.empty,
                    properties: Properties = new Properties()): SparkListenerJobStart = {
    SparkListenerJobStart(jobId, time, stageInfos, properties)
  }

  def sparkJobEndFromId(jobId: Int): SparkListenerJobEnd = {
    sparkJobEnd(jobId = jobId)
  }

  def sparkJobEnd(jobId: Int = 0,
                  time: Long = TEST_TIME_LONG,
                  jobResult: JobResult = JobSucceeded): SparkListenerJobEnd = {
    SparkListenerJobEnd(jobId, time, jobResult)
  }
}
