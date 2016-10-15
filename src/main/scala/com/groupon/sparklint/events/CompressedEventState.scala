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
import com.groupon.sparklint.data._
import com.groupon.sparklint.data.compressed._
import org.apache.spark.scheduler._

/**
  *
  * @author swhitear 
  * @since 9/7/16.
  */
class CompressedEventState(metricsBuckets: Int = 1000) extends EventStateLike {
  // collections will be optimized for fast writes
  // snapshoting will enable the analyzer to capture the raw data and run models against it
  var state: CompressedState = CompressedState.empty

  override def getState: SparklintStateLike = state

  override def unEvent(event: SparkListenerEvent): Unit = synchronized {
    event match {
      case event: SparkListenerApplicationStart  => unAddApp(event)
      case event: SparkListenerExecutorAdded     => unAddExecutor(event)
      case event: SparkListenerExecutorRemoved   => unRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => unAddBlockManager(event)
      case event: SparkListenerJobStart          => unJobStart(event)
      case event: SparkListenerStageSubmitted    => unStageSubmitted(event)
      case event: SparkListenerTaskStart         => unTaskStart(event)
      case event: SparkListenerTaskEnd           => unTaskEnd(event)
      case event: SparkListenerStageCompleted    => unStageCompleted(event)
      case event: SparkListenerJobEnd            => unJobEnd(event)
      case event: SparkListenerUnpersistRDD      => unUnpersistRDD(event)
      case event: SparkListenerApplicationEnd    => unEndApp(event)
      case _                                     =>
    }
  }

  override def onEvent(listenerEvent: SparkListenerEvent): Unit = synchronized {
    listenerEvent match {
      case event: SparkListenerApplicationStart  => addApp(event)
      case event: SparkListenerExecutorAdded     => addExecutor(event)
      case event: SparkListenerExecutorRemoved   => removeExecutor(event)
      case event: SparkListenerBlockManagerAdded => addBlockManager(event)
      case event: SparkListenerJobStart          => jobStart(event)
      case event: SparkListenerStageSubmitted    => stageSubmitted(event)
      case event: SparkListenerTaskStart         => taskStart(event)
      case event: SparkListenerTaskEnd           => taskEnd(event)
      case event: SparkListenerStageCompleted    => stageCompleted(event)
      case event: SparkListenerJobEnd            => jobEnd(event)
      case event: SparkListenerUnpersistRDD      => unpersistRDD(event)
      case event: SparkListenerApplicationEnd    => endApp(event)
      case _                                     =>
    }
  }

  private def addApp(event: SparkListenerApplicationStart): Unit = {
    state = state.copy(
      //appStart = Some(event),
      lastUpdatedAt = event.time
    )
  }

  private def unAddApp(event: SparkListenerApplicationStart): Unit = {
    state = state.copy(
      //appStart = None,
      lastUpdatedAt = event.time
    )
  }

  private def addExecutor(event: SparkListenerExecutorAdded): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo + (executorId -> SparklintExecutorInfo(event.executorInfo.totalCores, event.time, None)),
      lastUpdatedAt = event.time)
  }

  private def unAddExecutor(event: SparkListenerExecutorAdded): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo - executorId,
      lastUpdatedAt = event.time)
  }

  private def removeExecutor(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo + (executorId -> state.executorInfo(executorId).copy(endTime = Some(event.time))),
      lastUpdatedAt = event.time)
  }

  private def unRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo + (executorId -> state.executorInfo(executorId).copy(endTime = None)),
      lastUpdatedAt = event.time)
  }

  private def addBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  private def unAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  private def jobStart(event: SparkListenerJobStart): Unit = {}

  private def unJobStart(event: SparkListenerJobStart): Unit = {}

  private def stageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stageId = event.stageInfo.stageId
    val stageIdentifier = StageIdentifier.fromStageInfo(event.stageInfo, event.properties)
    state = state.copy(
      stageIdLookup = state.stageIdLookup + (stageId -> stageIdentifier),
      lastUpdatedAt = event.stageInfo.submissionTime.getOrElse(state.lastUpdatedAt))
  }

  private def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stageId = event.stageInfo.stageId
    state = state.copy(
      stageIdLookup = state.stageIdLookup - stageId,
      lastUpdatedAt = event.stageInfo.submissionTime.getOrElse(state.lastUpdatedAt))
  }

  private def stageCompleted(event: SparkListenerStageCompleted): Unit = {}

  private def unStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  private def taskStart(event: SparkListenerTaskStart): Unit = {
    val startTime = event.taskInfo.launchTime
    if (state.firstTaskAt.isEmpty) {
      state = state.copy(
        coreUsage = Utils.LOCALITIES.map(locality => locality -> CompressedMetricsSink.empty(startTime, metricsBuckets)).toMap,
        firstTaskAt = Some(startTime),
        runningTasks = Map(event.taskInfo.taskId -> SparklintTaskInfo(event.taskInfo)),
        lastUpdatedAt = startTime
      )
    } else {
      state = state.copy(
        runningTasks = state.runningTasks + (event.taskInfo.taskId -> SparklintTaskInfo(event.taskInfo)),
        lastUpdatedAt = startTime)
    }
  }

  private def unTaskStart(event: SparkListenerTaskStart): Unit = {
    val startTime = event.taskInfo.launchTime
    if (state.firstTaskAt.get == startTime) {
      state = state.copy(
        coreUsage = Map.empty,
        firstTaskAt = None,
        runningTasks = Map.empty,
        lastUpdatedAt = startTime
      )
    } else {
      state = state.copy(
        runningTasks = state.runningTasks - event.taskInfo.taskId,
        lastUpdatedAt = startTime)
    }
  }

  private def taskEnd(event: SparkListenerTaskEnd): Unit = {
    val stageId = state.stageIdLookup(event.stageId)
    val locality = event.taskInfo.taskLocality
    state = state.copy(
      coreUsage = state.coreUsage + (locality -> state.coreUsage(locality).addUsage(
        startTime = event.taskInfo.launchTime,
        endTime = event.taskInfo.finishTime)),
      runningTasks = state.runningTasks - event.taskInfo.taskId,
      stageMetrics = state.stageMetrics + (stageId -> state.stageMetrics.getOrElse(stageId, CompressedStageMetrics.empty).merge(
        taskId = event.taskInfo.taskId,
        taskType = Symbol(event.taskType),
        locality = event.taskInfo.taskLocality,
        metrics = event.taskMetrics)),
      lastUpdatedAt = event.taskInfo.finishTime)
  }

  private def unTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val locality = event.taskInfo.taskLocality
    state = state.copy(
      coreUsage = state.coreUsage + (locality -> state.coreUsage(locality).removeUsage(
        startTime = event.taskInfo.launchTime,
        endTime = event.taskInfo.finishTime)),
      runningTasks = state.runningTasks + (event.taskInfo.taskId -> SparklintTaskInfo(event.taskInfo)),
      // cannot undo message from stageMetrics, since it is not reversible
      lastUpdatedAt = event.taskInfo.finishTime)
  }

  private def jobEnd(event: SparkListenerJobEnd): Unit = {}

  private def unJobEnd(event: SparkListenerJobEnd): Unit = {}

  private def unpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  private def unUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  private def endApp(event: SparkListenerApplicationEnd): Unit = {
    state = state.copy(
      executorInfo = state.executorInfo.map(pair => {
        val executorInfo = if (pair._2.endTime.isEmpty) pair._2.copy(endTime = Some(event.time)) else pair._2
        pair._1 -> executorInfo
      }),
      applicationEndedAt = Some(event.time),
      lastUpdatedAt = event.time)
  }

  private def unEndApp(event: SparkListenerApplicationEnd): Unit = {
    state = state.copy(
      executorInfo = state.executorInfo.map(pair => {
        val executorInfo = if (pair._2.endTime.exists(_ == event.time)) pair._2.copy(endTime = None) else pair._2
        pair._1 -> executorInfo
      }),
      applicationEndedAt = None,
      lastUpdatedAt = event.time)
  }
}


