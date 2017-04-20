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

package com.groupon.sparklint.events

import java.io.{File, FileInputStream}
import java.util.UUID
import java.util.zip.ZipInputStream

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Roboxue on 2017/4/2.
  */
class EventSourceTest extends FlatSpec with Matchers {
  "fromFile" should "throw up if file is empty" in {
    val e = intercept[UnrecognizedLogFileException] {
      EventSource.fromFile(new File(getClass.getClassLoader.getResource("file_event_log_empty_test").getFile))
    }
    e.getMessage() shouldBe "file_event_log_empty_test can not be recognized as a spark log file. Reason: content is empty."
  }

  it should "throw up if file is not a valid log file" in {
    val e = intercept[UnrecognizedLogFileException] {
      EventSource.fromFile(new File(getClass.getClassLoader.getResource("invalid_log_file").getFile))
    }
    e.getMessage() shouldBe "invalid_log_file can not be recognized as a spark log file. " +
      "Reason: firstLine 'this is not a valid log file' is not a SparkListenerLogStart event."
  }

  it should "collect meta data correctly" in {
    val es = EventSource.fromFile(new File(getClass.getClassLoader.getResource("spark_event_log_example").getFile))
    es.appMeta shouldBe EventSourceMeta(Some("application_1462781278026_205691"), None, "MyAppName", Some("1.5.2"), 1466087746466L)
  }

  "fromZipStream" should "decompress a plain logfile zip" in {
    val file = getClass.getClassLoader.getResource("history_source/eventLogs-application_1489705648216_1600.zip").getFile
    val zipFile = new ZipInputStream(new FileInputStream(file))
    val es = EventSource.fromZipStream(zipFile, UUID.randomUUID().toString, "eventLogs-application_1489705648216_1600.zip")
    es.appMeta shouldBe EventSourceMeta(Some("application_1489705648216_1600"), None, "SEM_Feed_Generator", Some("1.6.1"), 1490175603867L)
  }
}
