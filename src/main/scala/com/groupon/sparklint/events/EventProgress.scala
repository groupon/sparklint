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

import scala.collection.mutable

/**
  * Simple data structure to track event source playback progress
  *
  * @author swhitear, rxue
  * @since 9/12/16.
  */
class EventProgress(var count: Int, var started: Int, var complete: Int, val active: mutable.Set[String]) {
  require(count >= 0)
  require(started >= 0 && started <= count)
  require(complete >= 0 && complete <= count)
  require(complete <= started)

  def description = s"Completed $complete / $count ($percent%) with $inFlightCount active$activeString."

  def percent: Long = ((complete / safeCount) * 100).round

  def safeCount: Double = if (count == 0) 1 else count

  def inFlightCount: Int = started - complete

  private def activeString = if (active.isEmpty) "" else s" (${active.mkString(", ")})"

  def hasNext: Boolean = complete < count

  def hasPrevious: Boolean = complete > 0

  override def toString: String = s"$complete of $count with $inFlightCount active"
}

object EventProgress {
  def empty(): EventProgress = new EventProgress(0, 0, 0, mutable.Set.empty)
}
