package com.groupon.sparklint.events

/**
  * @author swhitear
  * @since 8/18/16.
  */
case class SourceAndDetail(source: EventSourceLike, detail: EventSourceDetail) {
  val id = source.eventSourceId
}
