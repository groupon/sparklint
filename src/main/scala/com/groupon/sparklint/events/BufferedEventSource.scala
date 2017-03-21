package com.groupon.sparklint.events

import org.apache.spark.scheduler._

/**
  * An event source which maintains an internal buffer that can then be used
  * as an [[EventSourceLike]] implementation.
  *
  * @author superbobry
  * @since 1/11/17.
  */
private[events] abstract class BufferedEventSource extends FreeScrollEventSource {

  private val buffer = new EventBuffer(fillBuffer())
  private val fwdScroll = new ScrollHandler(buffer.next, onEvent, !buffer.hasNext)
  private val rewScroll = new ScrollHandler(buffer.previous, unEvent, !buffer.hasPrevious)

  @throws[IllegalArgumentException]
  def forwardEvents(count: Int = 1): Unit = fwdScroll.scroll(count)

  @throws[IllegalArgumentException]
  def rewindEvents(count: Int = 1): Unit = rewScroll.scroll(count)

  @throws[IllegalArgumentException]
  def forwardTasks(count: Int = 1): Unit = fwdScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerTaskEnd])

  @throws[IllegalArgumentException]
  def rewindTasks(count: Int = 1): Unit = rewScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerTaskStart])

  @throws[IllegalArgumentException]
  def forwardStages(count: Int = 1): Unit = fwdScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerStageCompleted])

  @throws[IllegalArgumentException]
  def rewindStages(count: Int = 1): Unit = rewScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerStageSubmitted])

  @throws[IllegalArgumentException]
  def forwardJobs(count: Int = 1): Unit = fwdScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerJobEnd])

  @throws[IllegalArgumentException]
  def rewindJobs(count: Int = 1): Unit = rewScroll.scroll(count, (evt) => evt.isInstanceOf[SparkListenerJobStart])

  override def toEnd(): Unit = fwdScroll.scroll(Int.MaxValue)

  override def toStart(): Unit = rewScroll.scroll(Int.MaxValue)

  override def hasNext: Boolean = buffer.hasNext

  override def hasPrevious: Boolean = buffer.hasPrevious

  protected def fillBuffer(): IndexedSeq[SparkListenerEvent]

  protected def onEvent(event: SparkListenerEvent): Unit

  protected def unEvent(event: SparkListenerEvent): Unit
}

