package com.groupon.sparklint.events

import java.io.File

import scala.util.{Failure, Success, Try}

/**
  * A specific implementation of EventSourceManager that creates FileEventSources with Losslesss state management.
  *
  * @author swhitear
  * @since 11/21/16.
  */
class FileEventSourceManager extends EventSourceManager {

  def addFile(sourceFile: File): Option[EventSourceLike] = {
    val meta = new EventSourceMeta()
    val progress = new EventProgressTracker()
    val stateManager = new LosslessStateManager()

    Try {
      FileEventSource(sourceFile, Seq(meta, progress, stateManager))
    } match {
      case Success(eventSource) =>
        logInfo(s"Successfully created file source ${sourceFile.getName}")
        val detail = EventSourceDetail(eventSource.eventSourceId, meta, progress, stateManager)
        Some(addEventSourceAndDetail(SourceAndDetail(eventSource, detail)))
      case Failure(ex)          =>
        logWarn(s"Failure creating file source from ${sourceFile.getName}: ${ex.getMessage}")
        None
    }
  }
}
