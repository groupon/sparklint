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
class SparklintShuffleReadCounter(_fetchWaitTime: StatCounter = StatCounter(),
                                  _localBlocksFetched: StatCounter = StatCounter(),
                                  _localBytesRead: StatCounter = StatCounter(),
                                  _recordsRead: StatCounter = StatCounter(),
                                  _remoteBlocksFetched: StatCounter = StatCounter(),
                                  _remoteBytesRead: StatCounter = StatCounter()) {
  def merge(that: SparklintShuffleReadMetrics): SparklintShuffleReadCounter = {
    _fetchWaitTime.merge(that.fetchWaitTime)
    _localBlocksFetched.merge(that.localBlocksFetched)
    _localBytesRead.merge(that.localBytesRead)
    _recordsRead.merge(that.recordsRead)
    _remoteBlocksFetched.merge(that.remoteBlocksFetched)
    _remoteBytesRead.merge(that.remoteBytesRead)
    this
  }

  def fetchWaitTime: StatCounter = _fetchWaitTime.copy()

  def localBlocksFetched: StatCounter = _localBlocksFetched.copy()

  def localBytesRead: StatCounter = _localBytesRead.copy()

  def recordsRead: StatCounter = _recordsRead.copy()

  def remoteBlocksFetched: StatCounter = _remoteBlocksFetched.copy()

  def remoteBytesRead: StatCounter = _remoteBytesRead.copy()
}
