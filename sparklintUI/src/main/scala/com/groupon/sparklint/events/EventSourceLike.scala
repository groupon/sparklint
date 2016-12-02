package com.groupon.sparklint.events

/**
  * The EventSourceLike provides a set of Spark events from a specific source.
  *
  * @author swhitear
  * @since 8/18/16.
  */
trait EventSourceLike {

  val eventSourceId: String

  def forwardIfPossible(): Unit = this match {
    case scrollable: FreeScrollEventSource => scrollable.toEnd()
    case _                                 =>
  }
}
