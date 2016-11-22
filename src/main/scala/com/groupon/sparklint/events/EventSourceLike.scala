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

import com.groupon.sparklint.common.Utils

/**
  * The EventSourceLike provides a set of Spark events from a specific source.
  *
  * @author swhitear 
  * @since 8/18/16.
  */
trait EventSourceLike {

  val appId: String

  def appName: String

  def startTime: Long

  def endTime: Long

  def nameOrId: String = if (missingName) appId else appName

  lazy val trimmedId = appId.replace(Utils.STANDARD_APP_PREFIX, "")

  private def missingName = appName.isEmpty || appName == Utils.UNKNOWN_STRING

  // TODO: move the above onto the metadata event receiver

  def forwardIfPossible() = this match {
    case scrollable: FreeScrollEventSource => scrollable.toEnd()
    case _                                 =>
  }
}


