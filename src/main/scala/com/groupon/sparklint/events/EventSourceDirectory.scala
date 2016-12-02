package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.common.Logging

/**
  * @author rxue
  * @since 9/22/16.
  */
class EventSourceDirectory(eventSourceManager: FileEventSourceManager, val dir: File, runImmediately: Boolean)
  extends Logging {

  implicit val logger: Logging = this

  private var loadedFileNames = Set.empty[String]

  def poll(): Unit = {
    newFiles.foreach(file => {
      eventSourceManager.addFile(file) match {
        case Some(fileSource) =>
          if (runImmediately) fileSource.forwardIfPossible()
          loadedFileNames = loadedFileNames + file.getName
        case None         =>
          logger.logWarn(s"Failed to construct source from ${file.getName}")
      }
    })
  }

  private def currentFiles = dir.listFiles().filter(_.isFile)

  private def newFiles = currentFiles.filter(f => !loadedFileNames.contains(f.getName))
}
