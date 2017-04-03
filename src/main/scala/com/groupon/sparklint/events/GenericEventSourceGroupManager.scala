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

import scala.collection.mutable

/**
  * @author rxue
  * @since 1.0.5
  */
class GenericEventSourceGroupManager(override val name: String, override val closeable: Boolean) extends EventSourceGroupManager {
  protected val eventSourceMap: mutable.Map[String, EventSource] = mutable.Map.empty

  /**
    * Register an event source
    *
    * @param es the [[EventSource]] to register
    * @return true if success, false if the event source has been registered already
    */
  def registerEventSource(es: EventSource): Boolean = {
    if (eventSourceMap.values.exists(_.appMeta == es.appMeta)) {
      false
    } else {
      eventSourceMap(es.uuid.toString) = es
      true
    }
  }

  override def containsEventSources(uuid: String): Boolean = eventSourceMap.contains(uuid)

  override def getFreeScrollEventSource(uuid: String): FreeScrollEventSource = getEventSources(uuid).asInstanceOf[FreeScrollEventSource]

  override def getEventSources(uuid: String): EventSource = eventSourceMap(uuid)

  override def eventSources: Seq[EventSource] = eventSourceMap.values.toList
}
