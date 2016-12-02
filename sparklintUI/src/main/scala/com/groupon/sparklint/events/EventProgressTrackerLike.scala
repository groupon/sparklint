package com.groupon.sparklint.events

/**
  * @author swhitear
  * @since 9/12/16.
  */
trait EventProgressTrackerLike {
  val eventProgress: EventProgress
  val taskProgress : EventProgress
  val stageProgress: EventProgress
  val jobProgress  : EventProgress
}
