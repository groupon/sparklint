package com.groupon.sparklint.data

import java.util.Properties

import org.apache.spark.executor.{InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics}
import org.apache.spark.scheduler.{StageInfo, TaskInfo}

/**
  * @author rxue
  * @since 12/4/16.
  */
object SparkToSparklint {
  def sparklintInputMetrics(i: InputMetrics): SparklintInputMetrics = {
    SparklintInputMetrics(i.bytesRead, i.recordsRead)
  }

  def sparklintOutputMetrics(o: OutputMetrics): SparklintOutputMetrics = {
    SparklintOutputMetrics(o.recordsWritten, o.bytesWritten)
  }

  def sparklintShuffleReadMetrics(s: ShuffleReadMetrics): SparklintShuffleReadMetrics = {
    SparklintShuffleReadMetrics(s.fetchWaitTime, s.localBlocksFetched, s.localBytesRead, s.recordsRead, s.remoteBlocksFetched, s.remoteBytesRead)
  }

  def sparklintShuffleWriteMetrics(s: ShuffleWriteMetrics): SparklintShuffleWriteMetrics = {
    SparklintShuffleWriteMetrics(s.shuffleBytesWritten, s.shuffleRecordsWritten, s.shuffleWriteTime)
  }

  def sparklintStageIdentifier(stageInfo: StageInfo, properties: Properties): SparklintStageIdentifier = {
    SparklintStageIdentifier(Symbol(properties.getProperty("spark.jobGroup.id", "")), Symbol(properties.getProperty("spark.job.description", "")), stageInfo.name)
  }

  def sparklintTaskInfo(taskInfo: TaskInfo): SparklintTaskInfo = {
    SparklintTaskInfo(taskInfo.taskId, taskInfo.executorId, taskInfo.index, taskInfo.attemptNumber, taskInfo.launchTime, Symbol(taskInfo.taskLocality.toString), taskInfo.speculative)
  }
}
