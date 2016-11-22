package com.groupon.sparklint.events

import java.io.File

import scala.util.{Failure, Success, Try}

/**
  * A specific implementation of EventSourceManager that creates FileEventSources with Losslesss state management.
  *
  * @author swhitear 
  * @since 11/21/16.
  */
class FileEventSourceManager extends EventSourceManager[File] {

  def newStateManager: EventStateManagerLike = new LosslessEventStateManager()

  override def constructDetails(sourceFile: File): Option[SourceAndDetail] = {
    val meta = new EventSourceMeta()
    val progress = new EventProgressTracker()
    val stateManager = newStateManager

    Try {
      FileEventSource(sourceFile, Seq(meta, progress, stateManager))
    } match {
      case Success(eventSource) =>
        logInfo(s"Successfully created file source ${sourceFile.getName}")
        val detail = EventSourceDetail(eventSource.eventSourceId, meta, progress, stateManager)
        Some(SourceAndDetail(eventSource, detail))
      case Failure(ex)          =>
        logWarn(s"Failure creating file source from ${sourceFile.getName}: ${ex.getMessage}")
        None
    }
  }
}
