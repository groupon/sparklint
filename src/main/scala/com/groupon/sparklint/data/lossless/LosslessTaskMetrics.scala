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
package com.groupon.sparklint.data.lossless

import com.groupon.sparklint.data._
import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.StatCounter

import scala.collection.concurrent.TrieMap

/**
  * @author rxue
  * @since 9/22/16.
  */
class LosslessTaskMetrics extends SparklintTaskMetrics {
  val metricsByTaskId = TrieMap.empty[Long, TaskMetrics]

  override def outputMetrics: SparklintOutputMetrics = {
    new SparklintOutputMetrics(
      outputMetrics.recordWritten,
      outputMetrics.bytesWritten)
  }

  override def inputMetrics: SparklintInputMetrics = {
    new SparklintInputMetrics(
      inputMetrics.recordsRead,
      inputMetrics.bytesRead
    )
  }

  override def shuffleReadMetrics: SparklintShuffleReadMetrics = {
    new SparklintShuffleReadMetrics(
      shuffleReadMetrics.fetchWaitTime,
      shuffleReadMetrics.localBlocksFetched,
      shuffleReadMetrics.localBytesRead,
      shuffleReadMetrics.recordsRead,
      shuffleReadMetrics.remoteBlocksFetched,
      shuffleReadMetrics.remoteBytesRead
    )
  }

  override def shuffleWriteMetrics: SparklintShuffleWriteMetrics = {
    new SparklintShuffleWriteMetrics(
      shuffleWriteMetrics.shuffleBytesWritten,
      shuffleWriteMetrics.shuffleRecordsWritten,
      shuffleWriteMetrics.shuffleWriteTime
    )
  }

  override def diskBytesSpilled: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.diskBytesSpilled.toDouble))
  }

  override def memoryBytesSpilled: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.memoryBytesSpilled.toDouble))
  }

  override def executorDeserializeTime: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.executorDeserializeTime.toDouble))
  }

  override def jvmGCTime: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.jvmGCTime.toDouble))
  }

  override def resultSerializationTime: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.resultSerializationTime.toDouble))
  }

  override def resultSize: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.resultSize.toDouble))
  }

  override def executorRunTime: StatCounter = {
    StatCounter(metricsByTaskId.values.map(_.executorRunTime.toDouble))
  }

  override def merge(taskId: Long, metrics: TaskMetrics): LosslessTaskMetrics = {
    metricsByTaskId(taskId) = metrics
    this
  }
}
