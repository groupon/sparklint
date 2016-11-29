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
package com.groupon.sparklint.data.compressed

import com.groupon.sparklint.data._
import org.apache.spark.executor._
import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 8/16/16.
  */
class CompressedTaskMetrics extends SparklintTaskMetrics {
  private val _outputMetrics           = new SparklintOutputMetrics()
  private val _inputMetrics            = new SparklintInputMetrics()
  private val _shuffleReadMetrics      = new SparklintShuffleReadMetrics()
  private val _shuffleWriteMetrics     = new SparklintShuffleWriteMetrics()
  private val _diskBytesSpilled        = StatCounter()
  private val _memoryBytesSpilled      = StatCounter()
  private val _executorDeserializeTime = StatCounter()
  private val _jvmGCTime               = StatCounter()
  private val _resultSerializationTime = StatCounter()
  private val _resultSize              = StatCounter()
  private val _executorRunTime         = StatCounter()

  override def outputMetrics: SparklintOutputMetrics = _outputMetrics

  override def inputMetrics: SparklintInputMetrics = _inputMetrics

  override def shuffleReadMetrics: SparklintShuffleReadMetrics = _shuffleReadMetrics

  override def shuffleWriteMetrics: SparklintShuffleWriteMetrics = _shuffleWriteMetrics

  override def diskBytesSpilled: StatCounter = _diskBytesSpilled.copy()

  override def memoryBytesSpilled: StatCounter = _memoryBytesSpilled.copy()

  override def executorDeserializeTime: StatCounter = _executorDeserializeTime.copy()

  override def jvmGCTime: StatCounter = _jvmGCTime.copy()

  override def resultSerializationTime: StatCounter = _resultSerializationTime.copy()

  override def resultSize: StatCounter = _resultSize.copy()

  override def executorRunTime: StatCounter = _executorRunTime.copy()

  def merge(taskId: Long, metrics: TaskMetrics): CompressedTaskMetrics = {
    _inputMetrics.merge(metrics.inputMetrics)
    _outputMetrics.merge(metrics.outputMetrics)
    _shuffleReadMetrics.merge(metrics.shuffleReadMetrics)
    _shuffleWriteMetrics.merge(metrics.shuffleWriteMetrics)
    _diskBytesSpilled.merge(metrics.diskBytesSpilled)
    _memoryBytesSpilled.merge(metrics.memoryBytesSpilled)
    _executorDeserializeTime.merge(metrics.executorDeserializeTime)
    _jvmGCTime.merge(metrics.jvmGCTime)
    _resultSerializationTime.merge(metrics.resultSerializationTime)
    _resultSize.merge(metrics.resultSize)
    _executorRunTime.merge(metrics.executorRunTime)
    this
  }
}
