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

package com.groupon.sparklint.data

import org.apache.spark.util.StatCounter

import scala.collection.mutable

/**
  * @author rxue
  * @since 9/22/16.
  */
class LosslessTaskCounter extends SparklintTaskCounter {
  val metricsByTaskId: mutable.Map[Long, SparklintTaskMetrics] = mutable.Map.empty

  override def outputMetrics: SparklintOutputCounter = {
    val found = metricsByTaskId.values.map(_.outputMetrics)
    new SparklintOutputCounter(
      StatCounter(found.map(_.recordsWritten.toDouble)),
      StatCounter(found.map(_.bytesWritten.toDouble)))
  }

  override def inputMetrics: SparklintInputCounter = {
    val found = metricsByTaskId.values.map(_.inputMetrics)
    found.foldLeft(new SparklintInputCounter)(_ merge _)
  }

  override def shuffleReadMetrics: SparklintShuffleReadCounter = {
    val found = metricsByTaskId.values.map(_.shuffleReadMetrics)
    new SparklintShuffleReadCounter(
      StatCounter(found.map(_.fetchWaitTime.toDouble)),
      StatCounter(found.map(_.localBlocksFetched.toDouble)),
      StatCounter(found.map(_.localBytesRead.toDouble)),
      StatCounter(found.map(_.recordsRead.toDouble)),
      StatCounter(found.map(_.remoteBlocksFetched.toDouble)),
      StatCounter(found.map(_.remoteBytesRead.toDouble))
    )
  }

  override def shuffleWriteMetrics: SparklintShuffleWriteCounter = {
    val found = metricsByTaskId.values.map(_.shuffleWriteMetrics)
    new SparklintShuffleWriteCounter(
      StatCounter(found.map(_.shuffleBytesWritten.toDouble)),
      StatCounter(found.map(_.shuffleRecordsWritten.toDouble)),
      StatCounter(found.map(_.shuffleWriteTime.toDouble))
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

  override def merge(taskId: Long, metrics: SparklintTaskMetrics): LosslessTaskCounter = {
    metricsByTaskId(taskId) = metrics
    this
  }
}
