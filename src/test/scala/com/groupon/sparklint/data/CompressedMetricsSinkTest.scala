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

package com.groupon.sparklint.data

import org.scalatest.{FlatSpec, Matchers}

/**
  * @author rxue
  * @since 8/18/16.
  */
class CompressedMetricsSinkTest extends FlatSpec with Matchers {
  val originTime = 1471478400000L // 2016-08-18 00:00:00
  it should "create an empty storage with the specified size and default resolution" in {
    val actual = CompressedMetricsSink.empty(originTime, 5)
    actual.storage.length shouldBe 5
    actual.resolution shouldBe 1
    actual.origin shouldBe 1471478400000L
  }

  it should "standardizing the origin time of the bucket" in {
    // the origin time of this sink is 30 minute after originTime, while the resolution is one hour
    // we expect the origin time used in the sink is floored by resolution
    val actual = new CompressedMetricsSink(3600000L, None, originTime + 1800000L, Array.fill(10)(0))
    actual.bucketStart shouldBe originTime
  }

  it should "upgrade resolution and merge buckets when doing compact" in {
    val before = Array[Long](1, 5, 7, 11, 13, 19, 23)
    val actual = new CompressedMetricsSink(20, Some(Interval(originTime + 75, originTime + 195)), originTime + 74, before).compactStorage(5)
    actual.length shouldBe before.length
    actual.sum shouldBe before.sum
    actual(0) shouldBe 1 + 5
    actual(1) shouldBe 7 + 11 + 13 + 19 + 23
    actual(2) shouldBe 0
    actual(3) shouldBe 0
    actual(4) shouldBe 0
    actual(5) shouldBe 0
    actual(6) shouldBe 0
  }

  val testSink = new CompressedMetricsSink(5, Some(Interval(originTime + 3, originTime + 38)), originTime, Array[Long](1, 5, 7, 11, 13, 19, 23, 0))

  it should "addUsage when a compact is not required, and the start/end fits into the same bucket" in {
    val actual = testSink.addUsage(originTime + 6, originTime + 9)
    actual.storage.sum shouldBe testSink.storage.sum + (9 - 6)
    actual(0) shouldBe testSink(0)
    actual(1) shouldBe testSink(1) + (9 - 6)
    actual(2) shouldBe testSink(2)
  }

  it should "addUsage when a compact is not required, and the start/end fits into neighbouring bucket" in {
    val actual = testSink.addUsage(originTime + 6, originTime + 13)
    actual.storage.sum shouldBe testSink.storage.sum + (13 - 6)
    actual(0) shouldBe testSink(0)
    actual(1) shouldBe testSink(1) + (10 - 6)
    actual(2) shouldBe testSink(2) + (13 - 10)
    actual(3) shouldBe testSink(3)
  }

  it should "addUsage when a compact is not required, and the start/end fits into a series (>2) buckets" in {
    val actual = testSink.addUsage(originTime + 6, originTime + 28)
    actual.storage.sum shouldBe testSink.storage.sum + (28 - 6)
    actual(0) shouldBe testSink(0)
    actual(1) shouldBe testSink(1) + (10 - 6)
    actual(2) shouldBe testSink(2) + 5
    actual(3) shouldBe testSink(3) + 5
    actual(4) shouldBe testSink(4) + 5
    actual(5) shouldBe testSink(5) + (28 - 25)
    actual(6) shouldBe testSink(6)
  }

  it should "addUsage when a compact is required" in {
    val actual = testSink.addUsage(originTime + 26, originTime + 68) // 48 > 5 (resolution) * 8 (storage.length), so a compact is performed
    actual.resolution shouldBe 20 // 20ms is the next resolution after 5ms
    actual.storage.sum shouldBe testSink.storage.sum + (68 - 26)
    actual(0) shouldBe testSink(0) + testSink(1) + testSink(2) + testSink(3)
    actual(1) shouldBe testSink(4) + testSink(5) + testSink(6) + testSink(7) + (40 - 26)
    actual(2) shouldBe 20
    actual(3) shouldBe (68 - 60)
    actual(4) shouldBe 0
    actual(5) shouldBe 0
    actual(6) shouldBe 0
    actual(7) shouldBe 0
  }

  it should "merge SelfCompactingMetricsSinks correctly" in {
    val sink1 = new CompressedMetricsSink(1000L, Some(Interval(2600, 8800)), 2500L, Array(1, 2, 3, 4, 5, 6, 7, 8))
    val sink2 = new CompressedMetricsSink(1000L, Some(Interval(8100, 13050)), 8000L, Array(11, 12, 13, 14, 15, 16))
    val sink3 = new CompressedMetricsSink(500L, Some(Interval(2600, 4300)), 2500L, Array(101, 102, 103, 104))
    val actual = MetricsSink.mergeSinks(Seq(sink1, sink2, sink3))
    actual.origin shouldBe 2500L
    actual.bucketStart shouldBe 2000L
    actual.bucketEnd shouldBe 14000L
    actual.length shouldBe 12
    actual.storage shouldBe Array(1 + 101, 2 + 102 + 103, 3 + 104, 4, 5, 6, 7 + 11, 8 + 12, 13, 14, 15, 16)
  }

  it should "convert to usage map correctly" in {
    testSink.convertToUsageDistribution shouldBe Map(0 -> 4, 5 -> 5, 1 -> 12, 2 -> 5, 3 -> 5, 4 -> 5)
  }
}
