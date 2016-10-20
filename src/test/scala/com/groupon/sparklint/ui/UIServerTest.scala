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
package com.groupon.sparklint.ui

import java.io.File

import com.groupon.sparklint.TestUtils
import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import com.groupon.sparklint.events.{FreeScrollEventSource, _}
import org.json4s.JObject
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * @author rxue
  * @since 8/23/16.
  */
class UIServerTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var evSource       : EventSourceLike with FreeScrollEventSource = _
  var evSourceManager: EventSourceManagerLike                     = new EventSourceManager()

  override protected def beforeEach(): Unit = {
    val evState = new CompressedEventState(30)
    val file = new File(TestUtils.resource("spark_event_log_example"))
    evSource = new FileEventSource(file, evState)
    evSourceManager.addEventSource(evSource)
  }

  it should "return limited information when most of the information are not available" in {
    pretty(reportEventSource(evSource)) shouldBe
      """{
        |  "appName" : "MyAppName",
        |  "appId" : "application_1462781278026_205691",
        |  "currentCores" : 0,
        |  "runningTasks" : 0,
        |  "lastUpdatedAt" : 0,
        |  "applicationLaunchedAt" : 1466087746466,
        |  "applicationEndedAt" : 1466088058982,
        |  "progress" : {
        |    "percent" : 0,
        |    "description" : "0 / 426 (0%)",
        |    "has_next" : true,
        |    "has_previous" : false
        |  }
        |}""".stripMargin
  }

  it should "return limited information after application was submitted" in {

    TestUtils.replay(evSource, count = 1)

    pretty(reportEventSource(evSource)) shouldBe
      """{
        |  "appName" : "MyAppName",
        |  "appId" : "application_1462781278026_205691",
        |  "currentCores" : 0,
        |  "runningTasks" : 0,
        |  "lastUpdatedAt" : 1466087746466,
        |  "applicationLaunchedAt" : 1466087746466,
        |  "applicationEndedAt" : 1466088058982,
        |  "progress" : {
        |    "percent" : 0,
        |    "description" : "1 / 426 (0%)",
        |    "has_next" : true,
        |    "has_previous" : true
        |  }
        |}""".stripMargin
  }

  it should "return limited information after first task was submitted" in {

    TestUtils.replay(evSource, count = 6)

    pretty(reportEventSource(evSource)) shouldBe
      """{
        |  "appName" : "MyAppName",
        |  "appId" : "application_1462781278026_205691",
        |  "allocatedCores" : 4,
        |  "executors" : [ {
        |    "executorId" : "2",
        |    "cores" : 2,
        |    "start" : 1466087808972
        |  }, {
        |    "executorId" : "1",
        |    "cores" : 2,
        |    "start" : 1466087811580
        |  } ],
        |  "currentCores" : 1,
        |  "runningTasks" : 1,
        |  "currentTaskByExecutor" : [ {
        |    "executorId" : "2",
        |    "tasks" : [ {
        |      "taskId" : 0,
        |      "executorId" : "2",
        |      "index" : 5,
        |      "attemptNumber" : 0,
        |      "launchTime" : 1466087848562,
        |      "locality" : "NODE_LOCAL",
        |      "speculative" : false
        |    } ]
        |  } ],
        |  "timeUntilFirstTask" : 102096,
        |  "timeSeriesCoreUsage" : [ {
        |    "time" : 1466087848562,
        |    "idle" : 0.51048,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  } ],
        |  "maxAllocatedCores" : 4,
        |  "coreUtilizationPercentage" : 0.0,
        |  "lastUpdatedAt" : 1466087848562,
        |  "applicationLaunchedAt" : 1466087746466,
        |  "applicationEndedAt" : 1466088058982,
        |  "progress" : {
        |    "percent" : 1,
        |    "description" : "6 / 426 (1%)",
        |    "has_next" : true,
        |    "has_previous" : true
        |  }
        |}""".stripMargin
  }

  it should "return full information after first task was finished" in {

    TestUtils.replay(evSource, count = 11)

    pretty(reportEventSource(evSource)) shouldBe
      """{
        |  "appName" : "MyAppName",
        |  "appId" : "application_1462781278026_205691",
        |  "allocatedCores" : 4,
        |  "executors" : [ {
        |    "executorId" : "2",
        |    "cores" : 2,
        |    "start" : 1466087808972
        |  }, {
        |    "executorId" : "1",
        |    "cores" : 2,
        |    "start" : 1466087811580
        |  } ],
        |  "currentCores" : 4,
        |  "runningTasks" : 4,
        |  "currentTaskByExecutor" : [ {
        |    "executorId" : "2",
        |    "tasks" : [ {
        |      "taskId" : 0,
        |      "executorId" : "2",
        |      "index" : 5,
        |      "attemptNumber" : 0,
        |      "launchTime" : 1466087848562,
        |      "locality" : "NODE_LOCAL",
        |      "speculative" : false
        |    }, {
        |      "taskId" : 4,
        |      "executorId" : "2",
        |      "index" : 7,
        |      "attemptNumber" : 0,
        |      "launchTime" : 1466087852107,
        |      "locality" : "RACK_LOCAL",
        |      "speculative" : false
        |    } ]
        |  }, {
        |    "executorId" : "1",
        |    "tasks" : [ {
        |      "taskId" : 2,
        |      "executorId" : "1",
        |      "index" : 6,
        |      "attemptNumber" : 0,
        |      "launchTime" : 1466087852066,
        |      "locality" : "RACK_LOCAL",
        |      "speculative" : false
        |    }, {
        |      "taskId" : 3,
        |      "executorId" : "1",
        |      "index" : 11,
        |      "attemptNumber" : 0,
        |      "launchTime" : 1466087852067,
        |      "locality" : "RACK_LOCAL",
        |      "speculative" : false
        |    } ]
        |  } ],
        |  "timeUntilFirstTask" : 102096,
        |  "timeSeriesCoreUsage" : [ {
        |    "time" : 1466087848500,
        |    "idle" : 0.4192,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 1.722,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087849000,
        |    "idle" : 0.1412,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 2.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087849500,
        |    "idle" : 0.1412,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 2.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087850000,
        |    "idle" : -1.8588,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 2.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087850500,
        |    "idle" : -1.8588,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 2.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087851000,
        |    "idle" : -1.8588,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 2.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087851500,
        |    "idle" : -1.8588,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 2.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087852000,
        |    "idle" : -0.5588,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.472,
        |    "rackLocal" : 0.228,
        |    "noPref" : 0.0
        |  } ],
        |  "cumulativeCoreUsage" : [ {
        |    "cores" : 2,
        |    "duration" : 3438
        |  }, {
        |    "cores" : 3,
        |    "duration" : 119
        |  } ],
        |  "maxConcurrentTasks" : 3,
        |  "maxAllocatedCores" : 4,
        |  "maxCoreUsage" : 3,
        |  "coreUtilizationPercentage" : 2.0228343806104134,
        |  "lastUpdatedAt" : 1466087852118,
        |  "applicationLaunchedAt" : 1466087746466,
        |  "applicationEndedAt" : 1466088058982,
        |  "progress" : {
        |    "percent" : 3,
        |    "description" : "11 / 426 (3%)",
        |    "has_next" : true,
        |    "has_previous" : true
        |  }
        |}""".stripMargin
  }

  it should "return full information after all event was replayed" in {

    TestUtils.replay(evSource)
    
    pretty(reportEventSource(evSource)) shouldBe
      """{
        |  "appName" : "MyAppName",
        |  "appId" : "application_1462781278026_205691",
        |  "allocatedCores" : 6,
        |  "executors" : [ {
        |    "executorId" : "2",
        |    "cores" : 2,
        |    "start" : 1466087808972,
        |    "end" : 1466088058982
        |  }, {
        |    "executorId" : "1",
        |    "cores" : 2,
        |    "start" : 1466087811580,
        |    "end" : 1466088058982
        |  }, {
        |    "executorId" : "3",
        |    "cores" : 2,
        |    "start" : 1466088056872,
        |    "end" : 1466088058982
        |  } ],
        |  "currentCores" : 0,
        |  "runningTasks" : 0,
        |  "currentTaskByExecutor" : [ ],
        |  "timeUntilFirstTask" : 102096,
        |  "timeSeriesCoreUsage" : [ {
        |    "time" : 1466087840000,
        |    "idle" : 3.7138999999999998,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.2861,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087850000,
        |    "idle" : 0.9091000000000005,
        |    "any" : 0.2376,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.4382,
        |    "rackLocal" : 2.4151,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087860000,
        |    "idle" : 0.3559000000000001,
        |    "any" : 3.2361,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.408,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087870000,
        |    "idle" : -8.999999999996788E-4,
        |    "any" : 4.0009,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087880000,
        |    "idle" : -4.999999999997229E-4,
        |    "any" : 4.0005,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087890000,
        |    "idle" : -9.999999999976694E-5,
        |    "any" : 4.0001,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087900000,
        |    "idle" : -4.999999999997229E-4,
        |    "any" : 4.0005,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087910000,
        |    "idle" : -3.9999999999995595E-4,
        |    "any" : 4.0004,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087920000,
        |    "idle" : 0.45040000000000013,
        |    "any" : 3.5496,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087930000,
        |    "idle" : 1.4790999999999999,
        |    "any" : 2.5209,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087940000,
        |    "idle" : 3.2167,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.4095,
        |    "rackLocal" : 0.3738,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087950000,
        |    "idle" : 0.9112,
        |    "any" : 1.9741,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 1.1147,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087960000,
        |    "idle" : -8.999999999996788E-4,
        |    "any" : 4.0009,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087970000,
        |    "idle" : -3.9999999999995595E-4,
        |    "any" : 4.0004,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087980000,
        |    "idle" : 1.3201999999999998,
        |    "any" : 2.6798,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466087990000,
        |    "idle" : 4.0,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466088000000,
        |    "idle" : 4.0,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466088010000,
        |    "idle" : 4.0,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466088020000,
        |    "idle" : 4.0,
        |    "any" : 0.0,
        |    "processLocal" : 0.0,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.0,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466088030000,
        |    "idle" : 3.4086333333333334,
        |    "any" : 0.0,
        |    "processLocal" : 0.0028,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 0.5935,
        |    "noPref" : 0.0
        |  }, {
        |    "time" : 1466088040000,
        |    "idle" : 2.8361333333333336,
        |    "any" : 0.0861,
        |    "processLocal" : 0.0398,
        |    "nodeLocal" : 0.0,
        |    "rackLocal" : 1.0429,
        |    "noPref" : 0.0
        |  } ],
        |  "cumulativeCoreUsage" : [ {
        |    "cores" : 0,
        |    "duration" : 40000
        |  }, {
        |    "cores" : 1,
        |    "duration" : 30000
        |  }, {
        |    "cores" : 2,
        |    "duration" : 1438
        |  }, {
        |    "cores" : 3,
        |    "duration" : 46873
        |  }, {
        |    "cores" : 4,
        |    "duration" : 90000
        |  } ],
        |  "idleTime" : 142096,
        |  "idleTimeSinceFirstTask" : 40000,
        |  "maxConcurrentTasks" : 4,
        |  "maxAllocatedCores" : 6,
        |  "maxCoreUsage" : 4,
        |  "coreUtilizationPercentage" : 0.5881725797287303,
        |  "lastUpdatedAt" : 1466088058982,
        |  "applicationLaunchedAt" : 1466087746466,
        |  "applicationEndedAt" : 1466088058982,
        |  "progress" : {
        |    "percent" : 100,
        |    "description" : "426 / 426 (100%)",
        |    "has_next" : false,
        |    "has_previous" : true
        |  }
        |}""".stripMargin
  }

  private def reportEventSource(evSource: EventSourceLike): JObject = {
    UIServer.reportJson(SparklintStateAnalyzer(evSource), evSource)
  }
}
