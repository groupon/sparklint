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

package com.groupon.sparklint.analyzer

/**
  * Case class used to contain the load distribution for one unit interval
  *
  * @author rxue
  * @since 9/23/16.
  *
  * @param time the starting time of the interval
  * @param allocated number of CPU millis allocated during this interval
  * @param any number of CPU millis used towards ANY locality spark tasks during this interval
  * @param processLocal number of CPU millis used towards PROCESS_LOCAL locality spark tasks during this interval
  * @param nodeLocal number of CPU millis used towards NODE_LOCAL locality spark tasks during this interval
  * @param rackLocal number of CPU millis used towards RACK_LOCAL locality spark tasks during this interval
  * @param noPref number of CPU millis used towards NO_PREF locality spark tasks during this interval
  */
case class CoreUsage(time: Long, allocated: Option[Double], any: Option[Double], processLocal: Option[Double],
                     nodeLocal: Option[Double], rackLocal: Option[Double], noPref: Option[Double]) {
  lazy val idle: Option[Double] = allocated.map(_ - utilized)

  lazy val utilized: Double = any.getOrElse(0.0) + processLocal.getOrElse(0.0) + nodeLocal.getOrElse(0.0) + rackLocal.getOrElse(0.0) + noPref.getOrElse(0.0)
}
