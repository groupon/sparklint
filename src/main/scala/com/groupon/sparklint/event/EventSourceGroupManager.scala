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

package com.groupon.sparklint.event

import java.io.{File, FileNotFoundException}

import scala.collection.mutable.ListBuffer

/**
  * Created by Roboxue on 2017/4/2.
  */
trait EventSourceGroupManager {
  /**
    * @return The display name of this manager
    */
  def name: String

  /**
    * @return if this manager can be closed by user
    */
  def closeable: Boolean

  def eventSources: Seq[EventSource]

}

/**
  * @throws FileNotFoundException if the folder provided doesn't exist or is a file
  * @param folder the folder of the log files
  */
@throws[FileNotFoundException]
class FolderEventSourceGroupManager(folder: File) extends GenericEventSourceGroupManager(folder.getName, true) {
  if (!folder.exists() || folder.isFile) {
    throw new FileNotFoundException(folder.getAbsolutePath)
  }

}

class GenericEventSourceGroupManager(override val name: String, override val closeable: Boolean) extends EventSourceGroupManager {
  private val esList: ListBuffer[EventSource] = ListBuffer.empty

  /**
    * Register an event source
    *
    * @param es the [[EventSource]] to register
    * @return true if success, false if the event source has been registered already
    */
  def registerEventSource(es: EventSource): Boolean = {
    if (esList.exists(_.appMeta == es.appMeta)) {
      false
    } else {
      esList.append(es)
      true
    }
  }

  override def eventSources: Seq[EventSource] = esList
}
