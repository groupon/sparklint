package com.groupon.sparklint.events

/**
  * @author rxue
  * @since 1.0.5
  */
trait FreeScrollEventSource extends EventSource {
  def rewind(): Boolean

  //noinspection AccessorLikeMethodIsUnit
  def toStart(): Unit

  //noinspection AccessorLikeMethodIsUnit
  def toEnd(): Unit

  def forwardEvents(count: Int): Unit

  def forwardJobs(count: Int): Unit

  def forwardStages(count: Int): Unit

  def forwardTasks(count: Int): Unit

  def rewindEvents(count: Int): Unit

  def rewindJobs(count: Int): Unit

  def rewindStages(count: Int): Unit

  def rewindTasks(count: Int): Unit
}
