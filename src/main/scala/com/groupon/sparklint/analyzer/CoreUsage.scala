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

import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.json4s.JsonAST.{JDouble, JObject}

/**
  * Case class used to contain the load distribution for one unit interval
  *
  * @author rxue
  * @since 9/23/16.
  * @param time       the starting time of the interval
  * @param allocated  number of CPU millis allocated during this interval
  * @param byLocality number of CPU millis used towards certain locality during this interval
  * @param byPool     number of CPU millis used towards certain pool during this interval
  */
case class CoreUsage(time: Long, allocated: Option[Double], byLocality: Map[TaskLocality, Double], byPool: Map[Symbol, Double]) {
  lazy val idle: Option[Double] = allocated.map(_ - utilized)

  lazy val utilized: Double = byLocality.values.sum

  lazy val jObjectByLocality = JObject(byLocality.map({ case (locality, duration) =>
    locality.toString.toLowerCase -> JDouble(duration)
  }).toSeq: _*)

  lazy val jObjectByPool = JObject(byPool.map({ case (pool, duration) =>
    pool.name -> JDouble(duration)
  }).toSeq: _*)
}
