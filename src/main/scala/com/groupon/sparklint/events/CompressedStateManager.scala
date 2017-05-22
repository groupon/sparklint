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
import com.groupon.sparklint.data._
import org.apache.spark.scheduler._

/**
  * @author rxue
  * @since 9/7/16.
  */
class CompressedStateManager(metricsBuckets: Int = 1000) extends EventStateManagerLike with EventReceiverLike {
  // collections will be optimized for fast writes
  // snapshoting will enable the analyzer to capture the raw data and run models against it
  private var state: CompressedState = CompressedState.empty

  override def getState: SparklintStateLike = state

  override def onAddApp(event: SparkListenerApplicationStart): Unit = {
    state = state.copy(
      lastUpdatedAt = event.time
    )
  }

  override def unAddApp(event: SparkListenerApplicationStart): Unit = {
    state = state.copy(
      lastUpdatedAt = event.time
    )
  }

  override def onAddExecutor(event: SparkListenerExecutorAdded): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo + (executorId -> SparklintExecutorInfo(event.executorInfo.totalCores, event.time, None)),
      lastUpdatedAt = event.time)
  }

  override def unAddExecutor(event: SparkListenerExecutorAdded): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo - executorId,
      lastUpdatedAt = event.time)
  }

  override def onRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo + (executorId -> state.executorInfo(executorId).copy(endTime = Some(event.time))),
      lastUpdatedAt = event.time)
  }

  override def unRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    state = state.copy(
      executorInfo = state.executorInfo + (executorId -> state.executorInfo(executorId).copy(endTime = None)),
      lastUpdatedAt = event.time)
  }

  override def onAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  override def unAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  override def onJobStart(event: SparkListenerJobStart): Unit = {}

  override def unJobStart(event: SparkListenerJobStart): Unit = {}

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stageId = event.stageInfo.stageId
    val stageIdentifier = SparkToSparklint.sparklintStageIdentifier(event.stageInfo, event.properties)
    state = state.copy(
      stageIdLookup = state.stageIdLookup + (stageId -> stageIdentifier),
      lastUpdatedAt = event.stageInfo.submissionTime.getOrElse(state.lastUpdatedAt))
  }

  override def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stageId = event.stageInfo.stageId
    state = state.copy(
      stageIdLookup = state.stageIdLookup - stageId,
      lastUpdatedAt = event.stageInfo.submissionTime.getOrElse(state.lastUpdatedAt))
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  override def unStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val startTime = event.taskInfo.launchTime
    if (state.firstTaskAt.isEmpty) {
      state = state.copy(
        coreUsageByLocality = Utils.LOCALITIES.map(locality => locality -> CompressedMetricsSink.empty(startTime, metricsBuckets)).toMap,
        firstTaskAt = Some(startTime),
        runningTasks = Map(event.taskInfo.taskId -> SparkToSparklint.sparklintTaskInfo(event.taskInfo)),
        lastUpdatedAt = startTime
      )
    } else {
      state = state.copy(
        runningTasks = state.runningTasks + (event.taskInfo.taskId -> SparkToSparklint.sparklintTaskInfo(event.taskInfo)),
        lastUpdatedAt = startTime)
    }
  }

  override def unTaskStart(event: SparkListenerTaskStart): Unit = {
    val startTime = event.taskInfo.launchTime
    if (state.firstTaskAt.get == startTime) {
      state = state.copy(
        coreUsageByLocality = Map.empty,
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

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val stageId = state.stageIdLookup(event.stageId)
    val locality = event.taskInfo.taskLocality
    val pool = stageId.pool
    val metricsSinkForPool = state.coreUsageByPool.getOrElse(pool, CompressedMetricsSink.empty(state.firstTaskAt.get, metricsBuckets))
    state = state.copy(
      coreUsageByLocality = state.coreUsageByLocality + (locality -> state.coreUsageByLocality(locality).addUsage(
        startTime = event.taskInfo.launchTime,
        endTime = event.taskInfo.finishTime)),
      coreUsageByPool = state.coreUsageByPool + (pool -> metricsSinkForPool.addUsage(
        startTime = event.taskInfo.launchTime,
        endTime = event.taskInfo.finishTime)),
      runningTasks = state.runningTasks - event.taskInfo.taskId,
      stageMetrics = state.stageMetrics + (stageId -> state.stageMetrics.getOrElse(stageId, CompressedStageMetrics.empty).merge(
        taskId = event.taskInfo.taskId,
        taskType = Symbol(event.taskType),
        locality = locality,
        metrics = SparkVersionSpecificToSparklint.sparklintTaskMetrics(event.taskMetrics))),
      lastUpdatedAt = event.taskInfo.finishTime)
  }

  override def unTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val stageId = state.stageIdLookup(event.stageId)
    val locality = event.taskInfo.taskLocality
    val pool = stageId.pool
    state = state.copy(
      coreUsageByLocality = state.coreUsageByLocality + (locality -> state.coreUsageByLocality(locality).removeUsage(
        startTime = event.taskInfo.launchTime,
        endTime = event.taskInfo.finishTime)),
      coreUsageByPool = state.coreUsageByPool + (pool -> state.coreUsageByPool(pool).removeUsage(
        startTime = event.taskInfo.launchTime,
        endTime = event.taskInfo.finishTime)),
      runningTasks = state.runningTasks + (event.taskInfo.taskId -> SparkToSparklint.sparklintTaskInfo(event.taskInfo)),
      // cannot undo message from stageMetrics, since it is not reversible
      lastUpdatedAt = event.taskInfo.finishTime)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {}

  override def unJobEnd(event: SparkListenerJobEnd): Unit = {}

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  override def unUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  override def onEndApp(event: SparkListenerApplicationEnd): Unit = {
    state = state.copy(
      executorInfo = state.executorInfo.map(pair => {
        val executorInfo = if (pair._2.endTime.isEmpty) pair._2.copy(endTime = Some(event.time)) else pair._2
        pair._1 -> executorInfo
      }),
      applicationEndedAt = Some(event.time),
      lastUpdatedAt = event.time)
  }

  override def unEndApp(event: SparkListenerApplicationEnd): Unit = {
    state = state.copy(
      executorInfo = state.executorInfo.map(pair => {
        val executorInfo = if (pair._2.endTime.exists(_ == event.time)) pair._2.copy(endTime = None) else pair._2
        pair._1 -> executorInfo
      }),
      applicationEndedAt = None,
      lastUpdatedAt = event.time)
  }
}
