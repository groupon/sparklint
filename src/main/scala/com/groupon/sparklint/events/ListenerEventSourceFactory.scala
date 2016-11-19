package com.groupon.sparklint.events

import com.groupon.sparklint.SparklintServer._

import scala.util.{Failure, Success, Try}

/**
  * An implementation of EventSourceFactory that constructs the EventSourceDetail instances from live streams.
  *
  * @author swhitear
  * @since 11/18/16.
  */
class ListenerEventSourceFactory extends EventSourceFactory[String] {

  def buildEventSourceDetail(sourceId: String): Option[EventSourceDetail] = {
    var eventSourceDetail: EventSourceDetail = null
    Try {
      val progressReceiver = new EventSourceProgress()
      val stateReceiver = new CompressedEventState()
      val eventReceivers = Seq(progressReceiver, stateReceiver)
      val eventSource = BufferedEventSource(sourceId, eventReceivers)
      eventSource.startConsuming()
      eventSourceDetail = EventSourceDetail(eventSource, stateReceiver, progressReceiver)
    } match {
      case Success(eventSource) =>
        logInfo(s"Successfully created buffered source $sourceId")
        Some(eventSourceDetail)
      case Failure(ex)          =>
        logWarn(s"Failure creating buffered source from $sourceId: ${ex.getMessage}")
        None
    }
  }
}
