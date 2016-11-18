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
package com.groupon.sparklint.events

import org.apache.spark.scheduler._

/**
  * The EventReceiverLike interface provides base event routing and handling for all the event receivers
  * that are injected into an EventSource.
  *
  * All base methods are no-ops, implementers can chose to implement only those they need, and compose freely
  * from other traits.  Note, the onPreprocEvent, onOnEvent and onUnEvent handlers are called for each inbound
  * preproc, on and un event before routing to the typed event handler for each, resulting in at least two
  * handler calls for each event type.
  *
  * @author swhitear 
  * @since 11/15/16.
  */
trait EventReceiverLike {

  // Always called event handlers of prepreoc, on and un

  protected def onPreprocEvent(event: SparkListenerEvent): Unit = {}

  protected def onOnEvent(event: SparkListenerEvent): Unit = {}

  protected def onUnEvent(event: SparkListenerEvent): Unit = {}


  // Preprocessing base no-ops

  protected def preprocAddApp(event: SparkListenerApplicationStart): Unit = {}

  protected def preprocAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  protected def preprocRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  protected def preprocAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  protected def preprocJobStart(event: SparkListenerJobStart): Unit = {}

  protected def preprocStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  protected def preprocTaskStart(event: SparkListenerTaskStart): Unit = {}

  protected def preprocTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  protected def preprocStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  protected def preprocJobEnd(event: SparkListenerJobEnd): Unit = {}

  protected def preprocUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  protected def preprocEndApp(event: SparkListenerApplicationEnd): Unit = {}


  // On events base no-ops

  protected def addApp(event: SparkListenerApplicationStart): Unit = {}

  protected def addExecutor(event: SparkListenerExecutorAdded): Unit = {}

  protected def removeExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  protected def addBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  protected def jobStart(event: SparkListenerJobStart): Unit = {}

  protected def stageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  protected def taskStart(event: SparkListenerTaskStart): Unit = {}

  protected def taskEnd(event: SparkListenerTaskEnd): Unit = {}

  protected def stageCompleted(event: SparkListenerStageCompleted): Unit = {}

  protected def jobEnd(event: SparkListenerJobEnd): Unit = {}

  protected def unpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  protected def endApp(event: SparkListenerApplicationEnd): Unit = {}


  // Un event base no-ops

  protected def unAddApp(event: SparkListenerApplicationStart): Unit = {}

  protected def unAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  protected def unRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  protected def unAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  protected def unJobStart(event: SparkListenerJobStart): Unit = {}

  protected def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  protected def unTaskStart(event: SparkListenerTaskStart): Unit = {}

  protected def unTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  protected def unStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  protected def unJobEnd(event: SparkListenerJobEnd): Unit = {}

  protected def unUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  protected def unEndApp(event: SparkListenerApplicationEnd): Unit = {}

  // Main event routers

  def preprocess(event: SparkListenerEvent): Unit = synchronized {
    onPreprocEvent(event)
    event match {
      case event: SparkListenerApplicationStart  => preprocAddApp(event)
      case event: SparkListenerExecutorAdded     => preprocAddExecutor(event)
      case event: SparkListenerExecutorRemoved   => preprocRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => preprocAddBlockManager(event)
      case event: SparkListenerJobStart          => preprocJobStart(event)
      case event: SparkListenerStageSubmitted    => preprocStageSubmitted(event)
      case event: SparkListenerTaskStart         => preprocTaskStart(event)
      case event: SparkListenerTaskEnd           => preprocTaskEnd(event)
      case event: SparkListenerStageCompleted    => preprocStageCompleted(event)
      case event: SparkListenerJobEnd            => preprocJobEnd(event)
      case event: SparkListenerUnpersistRDD      => preprocUnpersistRDD(event)
      case event: SparkListenerApplicationEnd    => preprocEndApp(event)
      case _                                     =>
    }
  }

  def onEvent(event: SparkListenerEvent): Unit = synchronized {
   onOnEvent(event)
   event match {
      case event: SparkListenerApplicationStart  => addApp(event)
      case event: SparkListenerExecutorAdded     => addExecutor(event)
      case event: SparkListenerExecutorRemoved   => removeExecutor(event)
      case event: SparkListenerBlockManagerAdded => addBlockManager(event)
      case event: SparkListenerJobStart          => jobStart(event)
      case event: SparkListenerStageSubmitted    => stageSubmitted(event)
      case event: SparkListenerTaskStart         => taskStart(event)
      case event: SparkListenerTaskEnd           => taskEnd(event)
      case event: SparkListenerStageCompleted    => stageCompleted(event)
      case event: SparkListenerJobEnd            => jobEnd(event)
      case event: SparkListenerUnpersistRDD      => unpersistRDD(event)
      case event: SparkListenerApplicationEnd    => endApp(event)
      case _                                     =>
    }
  }

  def unEvent(event: SparkListenerEvent): Unit = synchronized {
    onUnEvent(event)
    event match {
      case event: SparkListenerApplicationStart  => unAddApp(event)
      case event: SparkListenerExecutorAdded     => unAddExecutor(event)
      case event: SparkListenerExecutorRemoved   => unRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => unAddBlockManager(event)
      case event: SparkListenerJobStart          => unJobStart(event)
      case event: SparkListenerStageSubmitted    => unStageSubmitted(event)
      case event: SparkListenerTaskStart         => unTaskStart(event)
      case event: SparkListenerTaskEnd           => unTaskEnd(event)
      case event: SparkListenerStageCompleted    => unStageCompleted(event)
      case event: SparkListenerJobEnd            => unJobEnd(event)
      case event: SparkListenerUnpersistRDD      => unUnpersistRDD(event)
      case event: SparkListenerApplicationEnd    => unEndApp(event)
      case _                                     =>
    }
  }
}
