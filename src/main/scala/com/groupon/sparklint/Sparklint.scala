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

package com.groupon.sparklint

import com.groupon.sparklint.common.SparklintConfig
import com.groupon.sparklint.server.{AdhocServer, HeartBeatService, StaticFileService}

/**
  * The class that contains the backend and ui
  *
  * @author rxue
  * @since 1.0.5
  */
class Sparklint(config: SparklintConfig) extends AdhocServer
  with StaticFileService
  with HeartBeatService {
  val backend = new SparklintBackend()
  val frontend = new SparklintFrontend(backend)
  registerService("", frontend.uiService)
  registerService("/backend", backend.backendService)

  override def DEFAULT_PORT: Int = config.defaultPort

}
