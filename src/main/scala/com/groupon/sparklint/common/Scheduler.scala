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

import java.util.Timer

/**
  * A way of scheduling tasks to run on timers. Not intended as a high resolution component,
  * hence the use of multi-second period and delays.
  *
  * @author swhitear
  * @since 9/15/16.
  */
class Scheduler extends SchedulerLike with Logging {

  private val timer = new Timer()

  def scheduleTask[T](task: ScheduledTask[T]): Unit = {
    timer.schedule(task, timerVal(task.delaySeconds), timerVal(task.periodSeconds))
    logInfo(s"Scheduled task $task")
  }

  private def timerVal(taskVal: Int) = taskVal * 1000

  def cancelAll(): Unit = timer.cancel()
}


