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

/**
  * @author rxue
  * @since 9/23/16.
  */
class SparklintShuffleWriteCounter(_shuffleBytesWritten: StatCounter = StatCounter(),
                                   _shuffleRecordsWritten: StatCounter = StatCounter(),
                                   _shuffleWriteTime: StatCounter = StatCounter()) {
  def merge(that: SparklintShuffleWriteMetrics): SparklintShuffleWriteCounter = {
    _shuffleBytesWritten.merge(that.shuffleBytesWritten)
    _shuffleRecordsWritten.merge(that.shuffleRecordsWritten)
    _shuffleWriteTime.merge(that.shuffleWriteTime)
    this
  }

  def shuffleBytesWritten: StatCounter = _shuffleBytesWritten.copy()

  def shuffleRecordsWritten: StatCounter = _shuffleRecordsWritten.copy()

  def shuffleWriteTime: StatCounter = _shuffleWriteTime.copy()
}
