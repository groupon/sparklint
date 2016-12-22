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

import com.groupon.sparklint.common.ResourceHelper
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author rxue
  * @since 6/16/16.
  */
class SparklintListenerTest extends FlatSpec with BeforeAndAfterEach with Matchers {
  val sparkEventLogExample = ResourceHelper.getResourceSource(getClass.getClassLoader, "spark_event_log_example").getLines().toSeq

  var listener: SparklintListener = _

  override protected def beforeEach(): Unit = {
    val conf = new SparkConf()
    conf.set("sparklint.noserver", "true")
    listener = new SparklintListener(conf)
  }
}
