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

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{FlatSpec, Matchers}

/**
  * @author swhitear 
  * @since 9/15/16.
  */
class SchedulerTest extends FlatSpec with Matchers {

  implicit val logger: Logging = new Logging {}


  it should "schedule a task with period and no delay" in {

    val counter = new AtomicInteger(0)

    def set(): Unit = {
      counter.incrementAndGet()
    }

    val task = ScheduledTask[AtomicInteger]("test", set)
    val scheduler = new Scheduler()

    scheduler.scheduleTask(task)

    Thread.sleep(2250)
    scheduler.cancelAll()

    counter.intValue() shouldEqual 3

  }

  it should "schedule a task with period and delay" in {

    val counter = new AtomicInteger(0)

    def set(): Unit = {
      counter.incrementAndGet()
    }

    val task = ScheduledTask[AtomicInteger]("test", set, delaySeconds = 1)
    val scheduler = new Scheduler()

    scheduler.scheduleTask(task)

    Thread.sleep(2250)
    scheduler.cancelAll()

    counter.intValue() shouldEqual 2

  }
}
