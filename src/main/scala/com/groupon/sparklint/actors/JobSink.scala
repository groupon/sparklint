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

package com.groupon.sparklint.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.spark.scheduler._

import scala.collection.{SortedMap, mutable}

/**
  * @author rxue
  * @since 6/3/17.
  */
object JobSink {
  val name: String = "job"

  def props: Props = Props(new JobSink)

  case class GetCoreUsageByLocality(numOfBuckets: Int, since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class GetCoreUsageByJobGroup(numOfBuckets: Int, since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class GetCoreUsageByPool(numOfBuckets: Int, since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class CoreUsageResponse(data: SortedMap[Long, UsageByGroup])

  case object NoDataYet

  case class GetCoreUtilizationByLocality(since: Option[Long], until: Option[Long], replyTo: ActorRef)

  case class GetCoreUtilizationByJobGroup(since: Option[Long], untilNo: Option[Long], replyTo: ActorRef)

  case class GetCoreUtilizationByPool(since: Option[Long], untilNo: Option[Long], replyTo: ActorRef)

  case class CoreUtilizationResponse(data: Map[String, Double])

  //Data
  case class JobSummary(jobGroup: Option[String], jobDescription: Option[String], pool: Option[String], started: Long, ended: Option[Long] = None)

  case class UsageByGroup(byGroup: Map[String, Int], idle: Int)

}

class JobSink extends Actor {

  import JobSink._

  implicit val usageSummaryOrdering: Ordering[UsageSummary] = UsageSummary
  private val stageToJob: mutable.Map[Int, Int] = mutable.Map()
  private val runningJobs: mutable.Map[Int, JobSummary] = mutable.Map()
  private val finishedJobs: mutable.Map[Int, JobSummary] = mutable.Map()
  private val metricsByLocality: Map[String, LosslessMetricsSink] = TaskLocality.values.toSeq.map(locality =>
    locality.toString -> new LosslessMetricsSink).toMap
  private val metricsByJobGroup: mutable.Map[String, LosslessMetricsSink] = mutable.Map.empty
  private val metricsByPool: mutable.Map[String, LosslessMetricsSink] = mutable.Map.empty
  private val availableCoresMetrics = new LosslessMetricsSink()
  private val liveExecutors: mutable.Map[String, UsageSummary] = mutable.Map.empty
  private val runningTask: mutable.Map[Long, UsageSummary] = mutable.Map.empty

  override def receive: Receive = accumulateData orElse processQuery

  def accumulateData: PartialFunction[Any, Unit] = {
    case e: SparkListenerApplicationEnd =>
      for ((executorId, executorSummary) <- liveExecutors) {
        liveExecutors.remove(executorId)
        executorSummary.end = Some(e.time)
      }
      for ((jobId, jobSummary) <- runningJobs) {
        runningJobs.remove(jobId)
        finishedJobs(jobId) = jobSummary.copy(ended = Some(e.time))
      }
      for ((taskId, taskExecution) <- runningTask) {
        runningTask.remove(taskId)
        taskExecution.end = Some(e.time)
      }

    case e: SparkListenerExecutorAdded =>
      val newExecutorSummary = UsageSummary(Some(e.executorId.hashCode), e.time, weight = e.executorInfo.totalCores)
      liveExecutors(e.executorId) = newExecutorSummary
      availableCoresMetrics.push(newExecutorSummary)
    case e: SparkListenerExecutorRemoved =>
      for (executorSummary <- liveExecutors.remove(e.executorId)) {
        executorSummary.end = Some(e.time)
      }

    case e: SparkListenerJobStart =>
      runningJobs(e.jobId) = JobSummary(Option(e.properties.getProperty("spark.jobGroup.id")),
        Option(e.properties.getProperty("spark.job.description")),
        Option(e.properties.getProperty("spark.scheduler.pool")),e.time)
      for (stageId <- e.stageIds) {
        stageToJob(stageId) = e.jobId
      }
    case e: SparkListenerJobEnd =>
      for (jobSummary <- runningJobs.remove(e.jobId)) {
        finishedJobs(e.jobId) = jobSummary.copy(ended = Some(e.time))
      }
      stageToJob.retain((_, jobId) => jobId != e.jobId)

    case e: SparkListenerStageSubmitted =>
    // ignore for now
    case e: SparkListenerStageCompleted =>
    // ignore for now

    case e: SparkListenerTaskStart =>
      val newTaskExecution = UsageSummary(Some(e.taskInfo.taskId), e.taskInfo.launchTime)
      runningTask(e.taskInfo.taskId) = newTaskExecution
      metricsByLocality(e.taskInfo.taskLocality.toString).push(newTaskExecution)
      val jobGroup = stageToJob.get(e.stageId).flatMap(runningJobs.get).flatMap(_.jobGroup).getOrElse("default")
      metricsByJobGroup.getOrElseUpdate(jobGroup, new LosslessMetricsSink()).push(newTaskExecution)
      val pool = stageToJob.get(e.stageId).flatMap(runningJobs.get).flatMap(_.pool).getOrElse("default")
      metricsByPool.getOrElseUpdate(pool, new LosslessMetricsSink()).push(newTaskExecution)
    case e: SparkListenerTaskEnd =>
      for (taskExecution <- runningTask.remove(e.taskInfo.taskId)) {
        taskExecution.end = Some(e.taskInfo.finishTime)
      }
  }

  def processQuery: PartialFunction[Any, Unit] = {
    case GetCoreUsageByLocality(numOfBuckets, since, until, replyTo) =>
      replyTo ! getCoreUsage(numOfBuckets, since, until, metricsByLocality)

    case GetCoreUsageByJobGroup(numOfBuckets, since, until, replyTo) =>
      replyTo ! getCoreUsage(numOfBuckets, since, until, metricsByJobGroup.toMap)

    case GetCoreUsageByPool(numOfBuckets, since, until, replyTo) =>
      replyTo ! getCoreUsage(numOfBuckets, since, until, metricsByPool.toMap)

    case GetCoreUtilizationByLocality(since, until, replyTo) =>
      replyTo ! getCoreUtilization(since, until, metricsByLocality)

    case GetCoreUtilizationByJobGroup(since, until, replyTo) =>
      replyTo ! getCoreUtilization(since, until, metricsByJobGroup.toMap)

    case GetCoreUtilizationByPool(since, until, replyTo) =>
      replyTo ! getCoreUtilization(since, until, metricsByPool.toMap)
  }

  protected def getCoreUsage(numOfBuckets: Int, since: Option[Long], until: Option[Long], dataSeries: Map[String, LosslessMetricsSink]): Any = {
    if (dataSeries.forall(_._2.isEmpty)) {
      return NoDataYet
    }
    val earliestTime = since.getOrElse(dataSeries.flatMap(_._2.earliestTime).min)
    val latestTime = until.getOrElse(dataSeries.flatMap(_._2.latestTime).max)
    if (earliestTime >= latestTime) {
      return NoDataYet
    }
    val coreUsageByJobGroup = dataSeries.mapValues(_.collect(earliestTime, numOfBuckets, latestTime))
    val availableCores = availableCoresMetrics.collect(earliestTime, numOfBuckets, latestTime)
    val compiledData = availableCores.map { case (bucket, cores) =>
      val usageByJobGroup = dataSeries.keys.toSeq.flatMap(jobGroup => {
        coreUsageByJobGroup(jobGroup).get(bucket).filter(_ > 0).map(usage => jobGroup -> usage)
      }).toMap
      val idle = (cores - usageByJobGroup.values.sum) max 0
      bucket -> UsageByGroup(usageByJobGroup, idle)
    }
    CoreUsageResponse(compiledData)
  }

  protected def getCoreUtilization(since: Option[Long], until: Option[Long], dataSeries: Map[String, LosslessMetricsSink]): Any = {
    if (availableCoresMetrics.nonEmpty && dataSeries.exists(_._2.nonEmpty)) {
      val earliestTime = since.getOrElse(dataSeries.flatMap(_._2.earliestTime).min)
      val latestTime = until.getOrElse(dataSeries.flatMap(_._2.latestTime).max)
      val totalCoreSeconds = availableCoresMetrics.sum(earliestTime, latestTime)
      if (totalCoreSeconds == 0) {
      } else {
        CoreUtilizationResponse(dataSeries.mapValues(_.sum(earliestTime, latestTime).toDouble / totalCoreSeconds))
      }
    } else {
      NoDataYet
    }
  }
}
