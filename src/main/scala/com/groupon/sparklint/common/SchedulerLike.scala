package com.groupon.sparklint.common

/**
  *
  * @author swhitear
  * @since 9/15/16.
  */
trait SchedulerLike {
  def scheduleTask[T](task: ScheduledTask[T]): Unit

  def cancelAll(): Unit
}
