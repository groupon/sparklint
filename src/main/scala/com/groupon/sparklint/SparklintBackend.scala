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

package com.groupon.sparklint

import java.io.File

import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.event.{EventSourceGroupManager, FolderEventSourceGroupManager}
import org.http4s.HttpService

import scala.collection.mutable.ListBuffer

/**
  * The backend api
  *
  * @author rxue
  * @since 1.0.5
  */
class SparklintBackend
  extends Logging {
  private val esgManagers = ListBuffer.empty[EventSourceGroupManager]

  /**
    * @return All available [[EventSourceGroupManager]]
    */
  def listEventSourceGroupManagers: Seq[EventSourceGroupManager] = esgManagers

  /** Appends the given [[EventSourceGroupManager]]
    *
    * @param elems the [[EventSourceGroupManager]] to append.
    */
  def append(elems: EventSourceGroupManager*): Unit = esgManagers.append(elems: _*)

  def backendService: HttpService = ???

  protected def appendFolderManager(folder: File): Unit = {
    append(new FolderEventSourceGroupManager(folder))
  }

}
