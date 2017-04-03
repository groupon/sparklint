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

import org.apache.spark.scheduler.TaskLocality._

/**
  * @author rxue
  * @since 8/31/16.
  */
object Utils {
  val LOCALITIES: Seq[TaskLocality] = Seq(PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY)
  val UNKNOWN_STRING: String = "<unknown>"
  val UNKNOWN_NUMBER: Long = 0
  val STANDARD_APP_PREFIX: String = "application_"
}
