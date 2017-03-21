/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  private def currentFiles: Seq[File] = dir.listFiles().filter(_.isFile)

  private def newFiles = currentFiles.filter(f => !loadedFileNames.contains(f.getName))
}
