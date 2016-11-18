package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.SparklintServer._

import scala.util.{Failure, Success, Try}

/**
  * An implementation of EventSourceFactory that constructs the EventSourceDetail instances from event source files.
  *
  * @author swhitear 
  * @since 11/18/16.
  */
class FileEventSourceFactory extends EventSourceFactory[File] {

  def buildEventSourceDetail(sourceFile: File): Option[EventSourceDetail] = {
    var eventSourceDetail: EventSourceDetail = null
    Try {
      val progressReceiver = new EventSourceProgress()
      val stateReceiver = new LosslessEventState()
      val eventReceivers = Seq(progressReceiver, stateReceiver)
      val eventSource = FileEventSource(sourceFile, eventReceivers)
      eventSourceDetail = EventSourceDetail(eventSource, stateReceiver, progressReceiver)
    } match {
      case Success(eventSource) =>
        logInfo(s"Successfully created file source ${sourceFile.getName}")
        Some(eventSourceDetail)
      case Failure(ex)          =>
        logWarn(s"Failure creating file source from ${sourceFile.getName}: ${ex.getMessage}")
        None
    }
  }
}
