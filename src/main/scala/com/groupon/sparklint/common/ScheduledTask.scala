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

import java.util.TimerTask

import scala.util.{Failure, Success, Try}

/**
  * A convenient class to represent a job to be scheduled
  *
  * @param name          the name of the task
  * @param fn            task function
  * @param periodSeconds schedule interval
  * @param delaySeconds  initial job delay
  * @param logger        the logger used for logging
  * @tparam T the type of the task input
  * @author swhitear
  * @since 9/15/16.
  */
case class ScheduledTask[T](name: String, fn: () => Unit,
                            periodSeconds: Int = 1, delaySeconds: Int = 0)
                           (implicit logger: Logging)
  extends TimerTask {

  override def run(): Unit = Try({
    logger.logInfo(s"Executing ScheduledTask $name.")
    fn()
  }) match {
    case Success(_) => logger.logInfo(s"Execution of $name completed")
    case Failure(ex) => logger.logError(s"Execution of $name failed with exception.", ex)
  }
}
