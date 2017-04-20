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

package com.groupon.sparklint.common

import org.slf4j.{Logger, LoggerFactory}

/**
  * A trait for enabling logging. This is pretty much similar to what Spark does internally
  * We replicate it here since the Spark one is private
  *
  * @author rxue
  * @since 7/4/16.
  */
trait Logging {
  @transient private var _logger: Logger = null

  def logError(message: String): Unit = {
    log.error(message)
  }

  def logError(message: String, ex: Throwable): Unit = {
    log.error(message, ex)
  }

  protected def log: Logger = {
    if (_logger == null) {
      val logName = this.getClass.getName.stripSuffix("$")
      _logger = LoggerFactory.getLogger(logName)
    }
    _logger
  }

  def logWarn(message: String): Unit = {
    log.warn(message)
  }

  def logInfo(message: String): Unit = {
    log.info(message)
  }

  def logDebug(message: String): Unit = {
    log.debug(message)
  }

  def logTrace(message: String): Unit = {
    log.trace(message)
  }
}
