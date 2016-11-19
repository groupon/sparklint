package com.groupon.sparklint.events

/**
  *
  * @author swhitear 
  * @since 11/16/16.
  */
case class StubEventSource(appId: String, progress: EventSourceProgress, stateManager: EventStateManagerLike) extends EventSourceLike with FreeScrollEventSource {
  override def version: String = ???

  override def host: String = ???

  override def port: Int = ???

  override def maxMemory: Long = ???

  override def appName: String = ???

  override def user: String = ???

  override def startTime: Long = ???

  override def endTime: Long = ???

  override def fullName: String = ???

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
