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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{StageInfo, TaskInfo}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

/**
  * The default spark.JsonProtocol generates json with space in the field name. It's not ideal for frontend usage
  * This implementation organizes the data better for d3.js usage
  *
  * @author rxue
  * @since 6/15/16.
  */
object JsonProtocol {
  def taskMetricsToJson(taskMetrics: TaskMetrics): JObject = {
    ("ExecutorDeserializeTime" -> taskMetrics.executorDeserializeTime) ~
      ("ExecutionRunTime" -> taskMetrics.executorRunTime) ~
      ("GCTime" -> taskMetrics.jvmGCTime) ~
      ("ResultSize" -> taskMetrics.resultSize) ~
      ("ResultSerializationTime" -> taskMetrics.resultSerializationTime) ~
      ("DiskBytesSpilled" -> taskMetrics.diskBytesSpilled) ~
      ("MemoryBytesSpilled" -> taskMetrics.memoryBytesSpilled) ~
      ("OutputWritten" ->
        ("Records" -> taskMetrics.outputMetrics.recordsWritten) ~
          ("Bytes" -> taskMetrics.outputMetrics.bytesWritten)
      ) ~
      ("ShuffleWritten" ->
        ("Bytes" -> taskMetrics.shuffleWriteMetrics.bytesWritten) ~
          ("Records" -> taskMetrics.shuffleWriteMetrics.recordsWritten) ~
          ("Time" -> taskMetrics.shuffleWriteMetrics.writeTime)
      ) ~
      ("ShuffleRead" ->
        ("RemoteBytes" -> taskMetrics.shuffleReadMetrics.remoteBytesRead) ~
          ("RemoteBlocks" -> taskMetrics.shuffleReadMetrics.remoteBlocksFetched) ~
          ("LocalBytes" -> taskMetrics.shuffleReadMetrics.localBytesRead) ~
          ("LocalBlocks" -> taskMetrics.shuffleReadMetrics.localBlocksFetched) ~
          ("FetchWaitTime" -> taskMetrics.shuffleReadMetrics.fetchWaitTime) ~
          ("Records" -> taskMetrics.shuffleReadMetrics.recordsRead)
      ) ~
      ("InputRead" ->
        ("Bytes" -> taskMetrics.inputMetrics.bytesRead) ~
          ("Method" -> "Deprecated in 2.0.0") ~
          ("Records" -> taskMetrics.inputMetrics.recordsRead)
      )
  }

  def taskInfoToJson(taskInfo: TaskInfo): JObject = {
    ("Index" -> taskInfo.index) ~
      ("Attempt" -> taskInfo.attemptNumber) ~
      ("LaunchTime" -> taskInfo.launchTime) ~
      ("ExecutorId" -> taskInfo.executorId) ~
      ("Host" -> taskInfo.host) ~
      ("Locality" -> taskInfo.taskLocality.toString) ~
      ("Speculative" -> taskInfo.speculative) ~
      ("GettingResultTime" -> taskInfo.gettingResultTime) ~
      ("FinishTime" -> taskInfo.finishTime) ~
      ("Failed" -> taskInfo.failed)
  }

  def stageInfoToJson(stageInfo: StageInfo): JObject = {
    ("StageId" -> stageInfo.stageId) ~
      ("AttemptId" -> stageInfo.attemptId) ~
      ("NumTasks" -> stageInfo.numTasks) ~
      ("ParentIds" -> stageInfo.parentIds) ~
      ("CompletionTime" -> stageInfo.completionTime) ~
      ("FailureReason" -> stageInfo.failureReason) ~
      ("Name" -> stageInfo.name)
  }
}
