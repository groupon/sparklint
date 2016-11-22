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
package com.groupon.sparklint.analyzer

import com.groupon.sparklint.data._
import com.groupon.sparklint.data.compressed.CompressedMetricsSink
import com.groupon.sparklint.data.lossless.LosslessMetricsSink
//import com.groupon.sparklint.data.compressed._
import com.groupon.sparklint.events.{EventSourceMetaLike, EventStateManagerLike}
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.scheduler.TaskLocality._

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * An implementation of SparklintAnalyzerLike that can analyze a SparklintState
  *
  * @author rxue
  * @since 9/23/16.
  * @param source       the source to analyze
  * @param stateManager the state to analyze
  */
class SparklintStateAnalyzer(val source: EventSourceMetaLike, val stateManager: EventStateManagerLike)
  extends SparklintAnalyzerLike {
  val state = stateManager.getState

  override lazy val getCurrentCores: Option[Int] = getRunningTasks

  override lazy val getRunningTasks: Option[Int] = Some(state.runningTasks.size)

  override lazy val getCurrentTaskByExecutors: Option[Map[String, Iterable[SparklintTaskInfo]]] = {
    if (state.firstTaskAt.isEmpty) None
    else {
      Some(state.runningTasks.values.groupBy(_.executorId))
    }
  }

  override lazy val getExecutorInfo: Option[Map[String, SparklintExecutorInfo]] = {
    if (state.executorInfo.isEmpty) None
    else {
      Some(state.executorInfo)
    }
  }

  override lazy val getTimeUntilFirstTask: Option[Long] = Try {
    state.firstTaskAt.get - source.startTime
  }.toOption

  override lazy val getCoreUtilizationPercentage: Option[Double] = {
    getTimeSeriesCoreUsage.map(coreUsage => {
      val sumUtilizedTime: Double = coreUsage.map(_.utilized).sum
      val sumCpuTime: Double = coreUsage.flatMap(_.allocated).sum
      if (sumCpuTime == 0) 0.0 else sumUtilizedTime / sumCpuTime
    })
  }

  override lazy val getIdleTime: Option[Long] = Try {
    getIdleTimeSinceFirstTask.get + getTimeUntilFirstTask.get
  }.toOption

  override lazy val getIdleTimeSinceFirstTask: Option[Long] = getCumulativeCoreUsage.flatMap(_.get(0))

  override lazy val getMaxConcurrentTasks: Option[Int] = getCumulativeCoreUsage.map(_.keys.max)

  override lazy val getMaxAllocatedCores: Option[Int] = {
    val sink = getAllocatedCores(1000)
    if (sink.nonEmpty) {
      Some(sink.storage.map(_ / sink.resolution).max.toInt)
    } else {
      None
    }
  }

  override lazy val getMaxCoreUsage: Option[Int] = getMaxConcurrentTasks

  override lazy val getLastUpdatedAt: Option[Long] = Some(state.lastUpdatedAt)

  override def getLocalityStatsByStageIdentifier(stageIdentifier: StageIdentifier): Option[SparklintStageMetrics] = {
    state.stageMetrics.get(stageIdentifier)
  }

  // TODO: Rdd reference feature is under development
  override def getRDDReferencedMoreThan(times: Int): Option[Seq[SparklintRDDInfo]] = ???

  override lazy val getTimeSeriesCoreUsage: Option[Seq[CoreUsage]] = {
    if (state.firstTaskAt.isDefined) {
      val resolution = coreUsageWithRunningTasks.map(_._2.resolution).max
      val adjustedCoreUsage = coreUsageWithRunningTasks.mapValues(_.changeResolution(resolution))
      val dataRanges = coreUsageWithRunningTasks.flatMap(_._2.dataRange)
      if (dataRanges.isEmpty) {
        Some(Seq.empty)
      } else {
        val dataRange = dataRanges.reduce(_ merge _)
        val numBuckets = (dataRange.length / resolution + 1).toInt
        val bucketStart = dataRange.minimum - dataRange.minimum % resolution
        val allocatedCoresTimeSeries = getAllocatedCores(numBuckets)
        val timeSeries = ArrayBuffer.fill[CoreUsage](numBuckets)(null)
        Range(0, numBuckets).foreach(index => {
          val time = bucketStart + index * resolution
          timeSeries(index) = CoreUsage(time,
            allocatedCoresTimeSeries.getAvgValueForTime(time),
            adjustedCoreUsage(ANY).getAvgValueForTime(time),
            adjustedCoreUsage(PROCESS_LOCAL).getAvgValueForTime(time),
            adjustedCoreUsage(NODE_LOCAL).getAvgValueForTime(time),
            adjustedCoreUsage(RACK_LOCAL).getAvgValueForTime(time),
            adjustedCoreUsage(NO_PREF).getAvgValueForTime(time)
          )
        })
        Some(timeSeries)
      }
    } else {
      None
    }
  }

  override lazy val getCumulativeCoreUsage: Option[Map[Int, Long]] = {
    if (state.coreUsage.exists(_._2.nonEmpty)) {
      Some(aggregatedCoreUsage.convertToUsageDistribution)
    } else {
      None
    }
  }

  /**
    * Compress the time series of vcores allocated into an array with the length provided
    *
    * @param numBuckets divide the app's lifetime into roughly this number of buckets
    * @return the metricsSink that stores the number of CPU millis allocated for each interval
    */
  private[analyzer] def getAllocatedCores(numBuckets: Int): MetricsSink = {
    var sink = CompressedMetricsSink.empty(source.startTime, numBuckets)
    state.executorInfo.values.foreach(executorInfo => {
      sink = sink.addUsage(executorInfo.startTime, executorInfo.endTime.getOrElse(state.lastUpdatedAt), executorInfo.cores)
    })
    sink
  }

  private lazy val aggregatedCoreUsage: MetricsSink = {
    MetricsSink.mergeSinks(coreUsageWithRunningTasks.values)
  }

  private lazy val coreUsageWithRunningTasks: Map[TaskLocality, MetricsSink] = {
    if (state.runningTasks.isEmpty) {
      state.coreUsage
    } else {
      var toReturn = state.coreUsage
      state.runningTasks.values.foreach(runningTask => {
        val locality = TaskLocality.withName(runningTask.locality)
        toReturn = toReturn.updated(locality, toReturn(locality).addUsage(runningTask.launchTime, state.lastUpdatedAt))
      })
      toReturn
    }
  }
}
