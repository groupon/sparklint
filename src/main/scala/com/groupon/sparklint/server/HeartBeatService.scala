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

import org.http4s.HttpService
import org.http4s.dsl._

/**
  * A mix-in trait to provide heartbeat check service on /heartbeat
  *
  * @author rxue
  * @since 4/25/16.
  */
trait HeartBeatService {
  this: RoutingMap =>
  registerService("/heartbeat", heartBeatService)

  def heartBeatService = HttpService {
    case GET -> Root =>
      Ok("OK")
  }
}
