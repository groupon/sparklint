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

/**
  * An analyzer is responsible of providing useful stats after processing spark event logs
  *
  * @author rxue
  * @since 6/13/16.
  */
trait SparklintAnalyzerLike {
  // Metadata
  /**
    * @return Time when the last event was processed
    */
  def getLastUpdatedAt: Option[Long]

  /**
    * If the app is using fair scheduler, return all the pools being used. Otherwise, empty
    * @return
    */
  def getFairSchedulerPools: Seq[String]

  // Instantaneous stats
  /**
    * @return how many cores are currently being used
    */
  def getCurrentCores: Option[Int]

  /**
    * Can be different from `getCurrentCores` since one task can sometimes use more than 1 cores
    *
    * @return how many tasks are currently being executed.
    */
  def getRunningTasks: Option[Int]

  /**
    * How tasks are allocated on each executor
    *
    * @return ExecutorId -> [TaskInfo]
    */
  def getCurrentTaskByExecutors: Option[Map[String, Iterable[SparklintTaskInfo]]]

  /**
    * Get the cores for each executor allocated
    *
    * @return ExecutorId -> [ExecutorInfo]
    */
  def getExecutorInfo: Option[Map[String, SparklintExecutorInfo]]

  // App lifetime resource usage stats
  /**
    * @return the number of milliseconds between sparklint listener was created and received first task info
    */
  def getTimeUntilFirstTask: Option[Long]

  /**
    * @return cores -> milliseconds with this level of core usage
    */
  def getCumulativeCoreUsage: Option[Map[Int, Long]]

  /**
    * @return sum(idle time)/sum(allocated cpu time)
    */
  def getCoreUtilizationPercentage: Option[Double]

  /**
    * @return [avg load], the time information is contained inside CoreUsage already
    */
  def getTimeSeriesCoreUsage: Option[Seq[CoreUsage]]

  /**
    * @return the number of milliseconds when the app is totally idle
    */
  def getIdleTime: Option[Long]

  /**
    * @return the number of milliseconds when the app is totally idle after the first task is submitted
    */
  def getIdleTimeSinceFirstTask: Option[Long]

  /**
    * @return the highest concurrent tasks throughout the app history
    */
  def getMaxConcurrentTasks: Option[Int]

  /**
    * @return the highest allocated cores throughout the app history
    */
  def getMaxAllocatedCores: Option[Int]

  /**
    * @return the highest core usage throughout the app history
    */
  def getMaxCoreUsage: Option[Int]

  // App lifetime locality stats
  /**
    * @param stageIdentifier the identifier for a series of stages
    * @return for each locality level, the cumulative task metrics for that locality level
    */
  def getLocalityStatsByStageIdentifier(stageIdentifier: SparklintStageIdentifier): Option[SparklintStageMetrics]

  // RDD reference counters
  /**
    * @param times filter rdds that has been referenced more than this many times
    * @return rdd information
    */
  def getRDDReferencedMoreThan(times: Int): Option[Seq[SparklintRDDInfo]]
}
