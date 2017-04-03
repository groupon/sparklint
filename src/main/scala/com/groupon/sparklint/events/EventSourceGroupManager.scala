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

import java.util.UUID

/**
  * @author rxue
  * @since 1.0.5
  */
trait EventSourceGroupManager {
  val uuid: UUID = UUID.randomUUID()

  /**
    * @return The display name of this manager
    */
  def name: String

  /**
    * @return if this manager can be closed by user
    */
  def closeable: Boolean

  def eventSources: Seq[EventSource]

  def getEventSources(uuid: String): EventSource

  def getFreeScrollEventSource(uuid: String): FreeScrollEventSource

  def containsEventSources(uuid: String): Boolean

}
