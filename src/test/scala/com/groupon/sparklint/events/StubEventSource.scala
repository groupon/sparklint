package com.groupon.sparklint.events

/**
  * @author swhitear 
  * @since 11/16/16.
  */
case class StubEventSource(eventSourceId: String, receivers: Seq[EventReceiverLike])
  extends FreeScrollEventSource {

  @throws[IllegalArgumentException]
  override def forwardEvents(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindEvents(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def forwardTasks(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindTasks(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def forwardStages(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindStages(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def forwardJobs(count: Int): Unit = ???

  @throws[IllegalArgumentException]
  override def rewindJobs(count: Int): Unit = ???

  override def toEnd(): Unit = ???

  override def toStart(): Unit = ???

  override def hasNext: Boolean = ???

  override def hasPrevious: Boolean = ???
}
