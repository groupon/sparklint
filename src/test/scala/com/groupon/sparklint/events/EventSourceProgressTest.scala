/*
 Copyright 2016 Groupon, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.groupon.sparklint.events

import org.scalatest.{Matchers, FlatSpec}

/**
  * @author swhitear 
  * @since 9/14/16.
  */
class EventSourceProgressTest extends FlatSpec with Matchers {

  it should "throw up with invalid .ctor params" in {
    intercept[IllegalArgumentException] {
      EventSourceProgress(-1, 0)
    }
    intercept[IllegalArgumentException] {
      EventSourceProgress(0, -1)
    }
    intercept[IllegalArgumentException] {
      EventSourceProgress(10, 11)
    }
  }

  it should "set start state correctly" in {
    val progress = EventSourceProgress(10, 0)
    progress.hasNext shouldBe true
    progress.hasPrevious shouldBe false
    progress.percent shouldEqual 0
  }

  it should "set end state correctly" in {
    val progress = EventSourceProgress(10, 10)
    progress.hasNext shouldBe false
    progress.hasPrevious shouldBe true
    progress.percent shouldEqual 100
  }

  it should "set mid state correctly" in {
    val progress = EventSourceProgress(10, 4)
    progress.hasNext shouldBe true
    progress.hasPrevious shouldBe true
    progress.percent shouldEqual 40
  }

}
