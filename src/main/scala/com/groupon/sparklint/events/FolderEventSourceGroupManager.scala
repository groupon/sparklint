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

import java.io.{File, FileNotFoundException}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * @throws FileNotFoundException if the folder provided doesn't exist or is a file
  * @param folder the folder of the log files
  * @author rxue
  * @since 1.0.5
  */
@throws[FileNotFoundException]
class FolderEventSourceGroupManager(folder: File) extends GenericEventSourceGroupManager(folder.getName, true) {
  if (!folder.exists() || folder.isFile) {
    throw new FileNotFoundException(folder.getAbsolutePath)
  }

  private val ignoredFiles: mutable.Set[String] = mutable.Set.empty

  def pull(): Unit = {
    for (file <- folder.listFiles().filter(_.isFile).filter(f => !ignoredFiles.contains(f.getAbsolutePath))) {
      tryPullEventSource(file) match {
        case Success(_) | Failure(_: UnrecognizedLogFileException) =>
          ignoredFiles.add(file.getAbsolutePath)
        case _ =>
        // allow retry for failures other than UnrecognizedLogFileException
      }
    }
  }

  private def tryPullEventSource(file: File): Try[EventSource] = Try({
    val es = EventSource.fromFile(file)
    registerEventSource(es)
    es
  })
}
