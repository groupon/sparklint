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

import org.apache.spark.groupon.SparkListenerLogStartShim
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

  def preprocess(event: SparkListenerEvent): Unit = synchronized {
    onPreprocEvent(event)
    event match {
      case event: SparkListenerLogStartShim => preprocLogStart(event)
      case event: SparkListenerEnvironmentUpdate => preprocEnvironmentUpdate(event)
      case event: SparkListenerApplicationStart => preprocAddApp(event)
      case event: SparkListenerExecutorAdded => preprocAddExecutor(event)
      case event: SparkListenerExecutorRemoved => preprocRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => preprocAddBlockManager(event)
      case event: SparkListenerBlockManagerRemoved => preprocRemoveBlockManager(event)
      case event: SparkListenerJobStart => preprocJobStart(event)
      case event: SparkListenerStageSubmitted => preprocStageSubmitted(event)
      case event: SparkListenerTaskStart => preprocTaskStart(event)
      case event: SparkListenerTaskEnd => preprocTaskEnd(event)
      case event: SparkListenerStageCompleted => preprocStageCompleted(event)
      case event: SparkListenerJobEnd => preprocJobEnd(event)
      case event: SparkListenerUnpersistRDD => preprocUnpersistRDD(event)
      case event: SparkListenerApplicationEnd => preprocEndApp(event)
      case _ =>
    }
  }

  protected def onPreprocEvent(event: SparkListenerEvent): Unit = {}

  protected def preprocLogStart(event: SparkListenerLogStartShim): Unit = {}


  // Preprocessing base no-ops

  protected def preprocEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {}

  protected def preprocAddApp(event: SparkListenerApplicationStart): Unit = {}

  protected def preprocAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  protected def preprocRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  protected def preprocAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  protected def preprocRemoveBlockManager(event: SparkListenerBlockManagerRemoved): Unit = {}

  protected def preprocJobStart(event: SparkListenerJobStart): Unit = {}

  protected def preprocStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  protected def preprocTaskStart(event: SparkListenerTaskStart): Unit = {}

  protected def preprocTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  protected def preprocStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  protected def preprocJobEnd(event: SparkListenerJobEnd): Unit = {}

  protected def preprocUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  protected def preprocEndApp(event: SparkListenerApplicationEnd): Unit = {}

  def onEvent(event: SparkListenerEvent): Unit = synchronized {
    onOnEvent(event)
    event match {
      case event: SparkListenerLogStartShim => onLogStart(event)
      case event: SparkListenerEnvironmentUpdate => onEnvironmentUpdate(event)
      case event: SparkListenerApplicationStart => onAddApp(event)
      case event: SparkListenerExecutorAdded => onAddExecutor(event)
      case event: SparkListenerExecutorRemoved => onRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => onAddBlockManager(event)
      case event: SparkListenerBlockManagerRemoved => onRemoveBlockManager(event)
      case event: SparkListenerJobStart => onJobStart(event)
      case event: SparkListenerStageSubmitted => onStageSubmitted(event)
      case event: SparkListenerTaskStart => onTaskStart(event)
      case event: SparkListenerTaskEnd => onTaskEnd(event)
      case event: SparkListenerStageCompleted => onStageCompleted(event)
      case event: SparkListenerJobEnd => onJobEnd(event)
      case event: SparkListenerUnpersistRDD => onUnpersistRDD(event)
      case event: SparkListenerApplicationEnd => onEndApp(event)
      case _ =>
    }
  }


  // On events base no-ops

  protected def onOnEvent(event: SparkListenerEvent): Unit = {}

  protected def onLogStart(event: SparkListenerLogStartShim): Unit = {}

  protected def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {}

  protected def onAddApp(event: SparkListenerApplicationStart): Unit = {}

  protected def onAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  protected def onRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  protected def onAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  protected def onRemoveBlockManager(event: SparkListenerBlockManagerRemoved): Unit = {}

  protected def onJobStart(event: SparkListenerJobStart): Unit = {}

  protected def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  protected def onTaskStart(event: SparkListenerTaskStart): Unit = {}

  protected def onTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  protected def onStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  protected def onJobEnd(event: SparkListenerJobEnd): Unit = {}

  protected def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}


  // Un event base no-ops

  protected def onEndApp(event: SparkListenerApplicationEnd): Unit = {}

  def unEvent(event: SparkListenerEvent): Unit = synchronized {
    onUnEvent(event)
    event match {
      case event: SparkListenerLogStartShim => unLogStart(event)
      case event: SparkListenerEnvironmentUpdate => unEnvironmentUpdate(event)
      case event: SparkListenerApplicationStart => unAddApp(event)
      case event: SparkListenerExecutorAdded => unAddExecutor(event)
      case event: SparkListenerExecutorRemoved => unRemoveExecutor(event)
      case event: SparkListenerBlockManagerAdded => unAddBlockManager(event)
      case event: SparkListenerBlockManagerRemoved => unRemoveBlockManager(event)
      case event: SparkListenerJobStart => unJobStart(event)
      case event: SparkListenerStageSubmitted => unStageSubmitted(event)
      case event: SparkListenerTaskStart => unTaskStart(event)
      case event: SparkListenerTaskEnd => unTaskEnd(event)
      case event: SparkListenerStageCompleted => unStageCompleted(event)
      case event: SparkListenerJobEnd => unJobEnd(event)
      case event: SparkListenerUnpersistRDD => unUnpersistRDD(event)
      case event: SparkListenerApplicationEnd => unEndApp(event)
      case _ =>
    }
  }

  protected def onUnEvent(event: SparkListenerEvent): Unit = {}

  protected def unLogStart(event: SparkListenerLogStartShim): Unit = {}

  protected def unEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {}

  protected def unAddApp(event: SparkListenerApplicationStart): Unit = {}

  protected def unAddExecutor(event: SparkListenerExecutorAdded): Unit = {}

  protected def unRemoveExecutor(event: SparkListenerExecutorRemoved): Unit = {}

  protected def unAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {}

  protected def unRemoveBlockManager(event: SparkListenerBlockManagerRemoved): Unit = {}

  protected def unJobStart(event: SparkListenerJobStart): Unit = {}

  protected def unStageSubmitted(event: SparkListenerStageSubmitted): Unit = {}

  protected def unTaskStart(event: SparkListenerTaskStart): Unit = {}

  protected def unTaskEnd(event: SparkListenerTaskEnd): Unit = {}

  protected def unStageCompleted(event: SparkListenerStageCompleted): Unit = {}

  // Main event routers

  protected def unJobEnd(event: SparkListenerJobEnd): Unit = {}

  protected def unUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {}

  protected def unEndApp(event: SparkListenerApplicationEnd): Unit = {}
}
