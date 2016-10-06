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
package com.groupon.sparklint.data

import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.executor._
import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 9/23/16.
  */
trait SparklintTaskMetrics {
  def outputMetrics          : SparklintOutputMetrics
  def inputMetrics           : Map[DataReadMethod, SparklintInputMetrics]
  def shuffleReadMetrics     : SparklintShuffleReadMetrics
  def shuffleWriteMetrics    : SparklintShuffleWriteMetrics
  def diskBytesSpilled       : StatCounter
  def memoryBytesSpilled     : StatCounter
  def executorDeserializeTime: StatCounter
  def jvmGCTime              : StatCounter
  def resultSerializationTime: StatCounter
  def resultSize             : StatCounter
  def executorRunTime        : StatCounter
  def merge(taskId: Long, metrics: TaskMetrics): SparklintTaskMetrics
}

class SparklintOutputMetrics(_recordsWritten: StatCounter = StatCounter(),
                             _bytesWritten: StatCounter = StatCounter()) {
  def merge(that: OutputMetrics): SparklintOutputMetrics = {
    _recordsWritten.merge(that.recordsWritten)
    _bytesWritten.merge(that.bytesWritten)
    this
  }

  def recordWritten = _recordsWritten.copy()

  def bytesWritten = _bytesWritten.copy()
}

class SparklintInputMetrics(_bytesRead: StatCounter = StatCounter(),
                            _recordsRead: StatCounter = StatCounter()) {
  def merge(that: InputMetrics): SparklintInputMetrics = {
    _bytesRead.merge(that.bytesRead)
    _recordsRead.merge(that.recordsRead)
    this
  }

  def bytesRead = _bytesRead.copy()

  def recordsRead = _recordsRead.copy()
}

class SparklintShuffleReadMetrics(_fetchWaitTime: StatCounter = StatCounter(),
                                  _localBlocksFetched: StatCounter = StatCounter(),
                                  _localBytesRead: StatCounter = StatCounter(),
                                  _recordsRead: StatCounter = StatCounter(),
                                  _remoteBlocksFetched: StatCounter = StatCounter(),
                                  _remoteBytesRead: StatCounter = StatCounter()) {
  def merge(that: ShuffleReadMetrics): SparklintShuffleReadMetrics = {
    _fetchWaitTime.merge(that.fetchWaitTime)
    _localBlocksFetched.merge(that.localBlocksFetched)
    _localBytesRead.merge(that.localBytesRead)
    _recordsRead.merge(that.recordsRead)
    _remoteBlocksFetched.merge(that.remoteBlocksFetched)
    _remoteBytesRead.merge(that.remoteBytesRead)
    this
  }

  def fetchWaitTime = _fetchWaitTime.copy()

  def localBlocksFetched = _localBlocksFetched.copy()

  def localBytesRead = _localBytesRead.copy()

  def recordsRead = _recordsRead.copy()

  def remoteBlocksFetched = _remoteBlocksFetched.copy()

  def remoteBytesRead = _remoteBytesRead.copy()
}

class SparklintShuffleWriteMetrics(_shuffleBytesWritten: StatCounter = StatCounter(),
                                   _shuffleRecordsWritten: StatCounter = StatCounter(),
                                   _shuffleWriteTime: StatCounter = StatCounter()) {
  def merge(that: ShuffleWriteMetrics): SparklintShuffleWriteMetrics = {
    _shuffleBytesWritten.merge(that.shuffleBytesWritten)
    _shuffleRecordsWritten.merge(that.shuffleRecordsWritten)
    _shuffleWriteTime.merge(that.shuffleWriteTime)
    this
  }

  def shuffleBytesWritten = _shuffleBytesWritten.copy()

  def shuffleRecordsWritten = _shuffleRecordsWritten.copy()

  def shuffleWriteTime = _shuffleWriteTime.copy()
}

