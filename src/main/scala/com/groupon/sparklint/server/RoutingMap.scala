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

package com.groupon.sparklint.server

import com.groupon.sparklint.common.Logging
import org.http4s.HttpService
import org.http4s.server.Router

import scala.collection.mutable

/**
  * Simple middleware to route services and log
  *
  * @author rxue
  * @since 4/26/16.
  */
trait RoutingMap {
  this: Logging =>
  private lazy val routingMap = mutable.Map.empty[String, HttpService]

  def registerService(prefix: String, service: HttpService): Unit = {
    routingMap(prefix) = service
  }

  def router: HttpService = {
    val service = Router(routingMap.toSeq: _*)
    HttpService {
      case req =>
        logDebug(s"${req.method.name} ${req.uri.toString()}")
        service.run(req)
    }
  }

}
