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

package com.groupon.sparklint.actors

import akka.actor.{Actor, Props}
import org.apache.spark.groupon.SparkListenerLogStartShim

/**
  * @author rxue
  * @since 6/2/17.
  */
object VersionSink {
  val name: String = "version"

  def props: Props = Props(new VersionSink)

  trait Query extends SparklintLogProcessor.LogProcessorQuery

  case object GetVersion extends Query

  case class VersionResponse(version: String)

}

class VersionSink extends Actor {

  import VersionSink._

  private var version = "unknown"

  override def receive: Receive = {
    case logStart: SparkListenerLogStartShim =>
      version = logStart.sparkVersion
    case GetVersion =>
      sender() ! VersionResponse(version)
  }
}
