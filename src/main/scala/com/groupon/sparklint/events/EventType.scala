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

/**
  * A set of case classes that represent the available EventSource types.
  *
  * @author swhitear 
  * @since 11/7/16.
  */
abstract class EventType(description: String)

object EventType {
  val EVENTS = "Events"
  val TASKS = "Tasks"
  val STAGES = "Stages"
  val JOBS = "Jobs"

  val ALL_TYPES = Seq(EVENTS, TASKS, STAGES, JOBS)

  def fromString(eventType: String): EventType = eventType match {
    case EVENTS => Events
    case TASKS => Tasks
    case STAGES => Stages
    case JOBS => Jobs
  }
}

case object Events extends EventType(EventType.EVENTS)

case object Tasks extends EventType(EventType.EVENTS)

case object Stages extends EventType(EventType.EVENTS)

case object Jobs extends EventType(EventType.EVENTS)

