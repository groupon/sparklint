package com.groupon.sparklint.events

/**
  * An extension of EventSourceLike that provides two way scrolling by various event types.
  *
  * @author swhitear
  * @since 8/18/16.
  */
trait FreeScrollEventSource extends EventSourceLike {

  @throws[IllegalArgumentException]
  def forwardEvents(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def rewindEvents(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def forwardTasks(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def rewindTasks(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def forwardStages(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def rewindStages(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def forwardJobs(count: Int = 1): Unit

  @throws[IllegalArgumentException]
  def rewindJobs(count: Int = 1): Unit

  def toEnd(): Unit

  def toStart(): Unit

  def hasNext: Boolean

  def hasPrevious: Boolean
}
