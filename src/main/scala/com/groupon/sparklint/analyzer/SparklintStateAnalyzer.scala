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

package com.groupon.sparklint.analyzer

import com.groupon.sparklint.data._
import com.groupon.sparklint.events.EventSourceMeta
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.scheduler.TaskLocality._

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * An implementation of SparklintAnalyzerLike that can analyze a SparklintState
  *
  * @author rxue
  * @since 9/23/16.
  * @param meta  the source to analyze
  * @param state the state to analyze
  */
class SparklintStateAnalyzer(val meta: EventSourceMeta, val state: SparklintStateLike)
  extends SparklintAnalyzerLike {

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
    state.firstTaskAt.get - meta.startTime
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

  override lazy val getFairSchedulerPools: Seq[String] = state.coreUsageByPool.keys.map(_.name).toSeq

  override lazy val getTimeSeriesCoreUsage: Option[Seq[CoreUsage]] = {
    if (state.firstTaskAt.isDefined) {
      val resolution = coreUsageByLocalityWithRunningTasks.map(_._2.resolution).max
      val adjustedCoreUsageByLocality = coreUsageByLocalityWithRunningTasks.mapValues(_.changeResolution(resolution))
      val adjustedCoreUsageByPool = coreUsageByPoolWithRunningTasks.mapValues(_.changeResolution(resolution))
      val dataRanges = coreUsageByLocalityWithRunningTasks.flatMap(_._2.dataRange)
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
            adjustedCoreUsageByLocality.mapValues(_.getAvgValueForTime(time).getOrElse(0.0)),
            adjustedCoreUsageByPool.mapValues(_.getAvgValueForTime(time).getOrElse(0.0))
          )
        })
        Some(timeSeries)
      }
    } else {
      None
    }
  }
  override lazy val getCumulativeCoreUsage: Option[Map[Int, Long]] = {
    if (state.coreUsageByLocality.exists(_._2.nonEmpty)) {
      Some(aggregatedCoreUsage.convertToUsageDistribution)
    } else {
      None
    }
  }
  private lazy val aggregatedCoreUsage: MetricsSink = {
    MetricsSink.mergeSinks(coreUsageByLocalityWithRunningTasks.values)
  }
  private lazy val coreUsageByLocalityWithRunningTasks: Map[TaskLocality, MetricsSink] = {
    if (state.runningTasks.isEmpty) {
      state.coreUsageByLocality
    } else {
      var toReturn = state.coreUsageByLocality
      state.runningTasks.values.foreach(runningTask => {
        val locality = TaskLocality.withName(runningTask.locality.name)
        toReturn = toReturn.updated(locality, toReturn(locality).addUsage(runningTask.launchTime, state.lastUpdatedAt))
      })
      toReturn
    }
  }
  private lazy val coreUsageByPoolWithRunningTasks: Map[Symbol, MetricsSink] = {
    state.coreUsageByPool
    // FIXME: Currently we don't have pool name for running tasks
  }


  override def getLocalityStatsByStageIdentifier(stageIdentifier: SparklintStageIdentifier): Option[SparklintStageMetrics] = {
    state.stageMetrics.get(stageIdentifier)
  }

  // TODO: Rdd reference feature is under development
  override def getRDDReferencedMoreThan(times: Int): Option[Seq[SparklintRDDInfo]] = ???

  /**
    * Compress the time series of vcores allocated into an array with the length provided
    *
    * @param numBuckets divide the app's lifetime into roughly this number of buckets
    * @return the metricsSink that stores the number of CPU millis allocated for each interval
    */
  private[analyzer] def getAllocatedCores(numBuckets: Int): MetricsSink = {
    var sink = CompressedMetricsSink.empty(meta.startTime, numBuckets)
    state.executorInfo.values.foreach(executorInfo => {
      sink = sink.addUsage(executorInfo.startTime, executorInfo.endTime.getOrElse(state.lastUpdatedAt), executorInfo.cores)
    })
    sink
  }
}
