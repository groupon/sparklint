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

import com.groupon.sparklint.data.SparklintStateLike

/**
  * The EventSourceLike provides a replayable set of Spark events from a specific source.
  *
  * @author swhitear 
  * @since 8/18/16.
  */
trait EventSourceLike {

  private val STANDARD_APP_PREFIX = "application_"

  lazy val trimmedId = appId.replace(STANDARD_APP_PREFIX, "")

  def version: String

  def host: String

  def port: Int

  def maxMemory: Long

  def appId: String

  def appName: String

  def user: String

  def startTime: Long

  def endTime: Long

  def progress: EventSourceProgress

  def state: SparklintStateLike

  def fullName: String
}

/**
  * An eventSource that supports free scroll
  */
trait CanFreeScroll {
  self: EventSourceLike =>

  @throws[NoSuchElementException]
  def forward(count: Int = 1): EventSourceProgress

  @throws[NoSuchElementException]
  def rewind(count: Int = 1): EventSourceProgress

  @throws[scala.NoSuchElementException]
  def end(): EventSourceProgress

  @throws[scala.NoSuchElementException]
  def start(): EventSourceProgress
}
