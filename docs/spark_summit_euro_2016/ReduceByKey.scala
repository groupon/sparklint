package com.groupon.sparklint

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.parseJson

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * A very simple reduceByKey
  *
  * --repeat will execute the program x times
  * --persistInput will persist the input dataset
  *
  * We will verify that persist speeds up execution and change the job's locality level
  *
  * @author rxue
  * @since 9/6/16.
  */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    println(s"Job started with args: ${args.mkString(",")}")
    val argument = new JobArgument(args)

    val input = sc.textFile("/user/rxue/sparklint_sample_input")
      .map(row => parseJson(row).extract[Record](DefaultFormats, manifest[Record]))
    if (argument.persistInput) {
      input.setName("input").persist()
    }

    for (i <- Range(0, argument.repeat)) {
      if (argument.parallelExecution) {
        val parallelExecutionContext = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(3))
        Await.ready(Future.sequence(Seq(
          Future(countByIp(input))(parallelExecutionContext),
          Future(countByStatus(input))(parallelExecutionContext),
          Future(countByVerb(input))(parallelExecutionContext)
        )), Duration.Inf)
      } else {
        countByIp(input)
        countByStatus(input)
        countByVerb(input)
      }
    }
    sc.clearJobGroup()
  }

  def countByIp(input: RDD[Record]) = {
    val summary = input.map(record => record.ip -> 1).reduceByKey(_ + _)
    input.sparkContext.setJobGroup("CountByIp", "CountByIp")
    summary.count()
    input.sparkContext.setJobDescription("TopIp")
    summary.sortBy(_._2, ascending = false).first()
  }

  def countByStatus(input: RDD[Record]) = {
    val summary = input.map(record => record.status -> 1).reduceByKey(_ + _)
    input.sparkContext.setJobGroup("CountByStatusCode", "CountByStatusCode")
    summary.count()
    input.sparkContext.setJobDescription("TopStatusCode")
    summary.sortBy(_._2, ascending = false).first()
  }

  def countByVerb(input: RDD[Record]) = {
    val summary = input.map(record => record.verb -> 1).reduceByKey(_ + _)
    input.sparkContext.setJobGroup("CountByVerb", "CountByVerb")
    summary.count()
    input.sparkContext.setJobDescription("TopVerb")
    summary.sortBy(_._2, ascending = false).first()
  }
}
