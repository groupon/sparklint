/*
 Copyright 2016 Groupon, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.groupon.sparklint.events

import com.groupon.sparklint.common.Utils._

import scala.collection.Map
import scala.collection.immutable.HashMap

/**
  * Pushing down some option wrangling, defaults, and base state handling to use across all
  * EvenSource impls.
  *
  * @author swhitear 
  * @since 9/29/16.
  */
abstract class EventSourceBase(eventState: EventStateLike) extends EventSourceLike {

  type EnvironmentData = Map[String, Seq[(String, String)]]

  val appId: String // still abstract here

  var appNameOpt: Option[String] = None

  def appName: String = appNameOpt.getOrElse(UNKNOWN_STRING)

  var userOpt: Option[String] = None

  def user: String = userOpt.getOrElse(UNKNOWN_STRING)

  var versionOpt: Option[String] = None

  def version: String = versionOpt.getOrElse(UNKNOWN_STRING)

  var hostOpt: Option[String] = None

  def host: String = hostOpt.getOrElse(UNKNOWN_STRING)

  var portOpt: Option[Int] = None

  def port: Int = portOpt.getOrElse(UNKNOWN_NUMBER.toInt)

  var maxMemoryOpt: Option[Long] = None

  def maxMemory: Long = maxMemoryOpt.getOrElse(UNKNOWN_NUMBER)

  var startTimeOpt: Option[Long] = None

  def startTime: Long = startTimeOpt.getOrElse(UNKNOWN_NUMBER)

  var endTimeOpt: Option[Long] = None

  def endTime: Long = endTimeOpt.getOrElse(UNKNOWN_NUMBER)

  var environmentOpt: Option[EnvironmentData] = None

  def environment: EnvironmentData = environmentOpt.getOrElse(HashMap.empty)

  def fullName = s"$appName ($appId)"

}