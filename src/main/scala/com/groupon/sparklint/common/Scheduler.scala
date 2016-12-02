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

  def cancelAll(): Unit = timer.cancel()

  private def timerVal(taskVal: Int) = taskVal * 1000
}


