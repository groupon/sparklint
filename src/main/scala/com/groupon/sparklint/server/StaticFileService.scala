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

import org.http4s.dsl._
import org.http4s.{HttpService, StaticFile}

import scalaz.concurrent.Task

/**
  * A mix-in trait to provide static file service on /static
  *
  * @author rxue
  * @since 5/16/16.
  */
trait StaticFileService {
  this: RoutingMap =>
  registerService("/static", staticFileService)

  def staticFileService = HttpService {
    case req@GET -> path =>
      StaticFile.fromResource(path.toString, Some(req)).fold(NotFound())(Task.now)
  }
}
