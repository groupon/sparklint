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

package com.groupon.sparklint.ui

import com.groupon.sparklint.events.EventType

import scala.xml.Node

/**
  * @author rxue
  * @since 6/14/16.
  */
class SparklintHomepage extends UITemplate {
  /**
    * These are all the frontend libraries used by Sparklint UI
    * jquery (dom operation, required by d3)
    * bootstrap framework (UI, out of box responsive design)
    * moment (time processing)
    * underscore (data processing)
    *
    * @return
    */

  override val title: String = "Sparklint"
  override val description: String = "Performance Analyzer for Apache Spark"
  override val author: String = "Groupon"

  override def content: Seq[Node] =
    <div id="wrapper">
      {navbar}<div id="page-wrapper">
      {mainContainer}
    </div>
    </div>

  def navbar: Seq[Node] =
    <nav class="navbar navbar-default navbar-static-top" role="navigation" style="margin-bottom: 0">
      <div class="navbar-header">
        <a class="navbar-brand" href="/">Sparklint</a>
      </div>
      <!-- Top Menu Items -->
      <ul class="nav navbar-top-links navbar-right">
      </ul>
      <!-- Sidebar Menu Items -->
      <div class="navbar-default sidebar" role="navigation">
        <div class="sidebar-nav navbar-collapse">
          <ul class="nav" id="side-menu">
            {addEventSourceGroupManagerControl}{navbarReplayControl}
          </ul>
        </div>
      </div>
    </nav>

  //noinspection MutatorLikeMethodIsParameterless
  def addEventSourceGroupManagerControl: Seq[Node] =
    <li id="addEventSourceGroupManager">
      <a href="#">
        <i class="fa fa-plus fa-fw"></i>
        Add Sources
        <span class="fa arrow"></span>
      </a>
      <ul class="nav nav-second-level collapse">
        <li>
          <a href="#" id="addSingleFile">Single File</a>
        </li>
        <li>
          <a href="#" id="addDirectory">Folder</a>
        </li>
        <li>
          <a href="#" id="addHistoryServer">History Server</a>
        </li>
      </ul>
    </li>

  def navbarReplayControl: Seq[Node] =
    <li id="navbarPlay">
      <div class="input-group custom-search-form disabled" id="replay-controls" style="display:None">
        <!--<form role="form"><fieldset disabled="true">-->
        <div class="input-group-btn">
          <button type="button" class="btn btn-default" title="Start" id="eventsToStart">
            <i class="fa fa-fast-backward"></i>
          </button>
          <button type="button" class="btn btn-default" title="Back" id="eventsBackward">
            <i class="fa fa-step-backward"></i>
          </button>
        </div>
        <select class="form-control" id="countSelector">
          <option>1</option>
          <option>5</option>
          <option>10</option>
          <option>25</option>
          <option>50</option>
          <option>100</option>
          <option>250</option>
          <option>1000</option>
        </select>
        <select class="form-control" id="typeSelector">
          {for (navType <- EventType.ALL_TYPES) yield
          <option>
            {navType}
          </option>}
        </select>
        <div class="input-group-btn">
          <button type="button" class="btn btn-default" title="Forward" id="eventsForward">
            <i class="fa fa-step-forward"></i>
          </button>
          <button type="button" class="btn btn-default" title="End" id="eventsToEnd">
            <i class="fa fa-fast-forward"></i>
          </button>
        </div>
      </div>
    </li>

  def mainContainer: Seq[Node] =
    <div class="container-fluid">
      <div class="row">
        <div class="col-md-12">
          <div class="alert alert-danger" id="error-message">
          </div>
        </div>
        <div class="col-md-12">
          <h1 class="page-header">
            <span id="appName">Sparklint</span>
            <small id="appId">Select an app from left side</small>
          </h1>
        </div>
        <div class="col-md-12">
          <div class="alert alert-success" id="summaryApplicationEndedAlert">
            <span id="summaryApplicationDuration"></span>
          </div>
          <div class="progress">
            <div id="summaryApplicationProgress" class="progress-bar" role="progressbar"
                 aria-valuenow="0" aria-valuemin="0" aria-valuemax="100" style="width: 0%">
              <span class="sr-only"></span>
            </div>
          </div>
        </div>
      </div>{summaryRow}<div class="row">
      <div class="col-lg-12">
        {coreUsageTimeSeriesByLocality}
        {coreUsageTimeSeriesByPool}
        {coreUsageDistribution}
        {taskDistributionList}
      </div>
    </div>
    </div>
      <div class="loading-spinner">
      </div>

  def summaryRow: Seq[Node] =
    <div id="summaryRow" class="row" style="display:None">
      <div class="col-lg-3 col-md-6" id="summaryExecutorPanel">
        <div class="panel panel-primary">
          <div class="panel-heading">
            <div class="row">
              <div class="col-xs-3">
                <i class="fa fa-comments fa-5x"></i>
              </div>
              <div class="col-xs-9 text-right">
                <div class="huge" id="summaryNumExecutors">--</div>
                <div>Executors</div>
              </div>
            </div>
          </div>
          <a href="#">
            <div class="panel-footer">
              <span class="pull-left">View Executors</span>
              <span class="pull-right">
                <i class="fa fa-arrow-circle-right"></i>
              </span>
              <div class="clearfix"></div>
            </div>
          </a>
        </div>
      </div>
      <div class="col-lg-3 col-md-6" id="summaryTaskPanel">
        <div class="panel panel-green">
          <div class="panel-heading">
            <div class="row">
              <div class="col-xs-3">
                <i class="fa fa-tasks fa-5x"></i>
              </div>
              <div class="col-xs-9 text-right">
                <div class="huge" id="summaryNumTasks">--</div>
                <div>Running Tasks</div>
              </div>
            </div>
          </div>
          <a href="#chartTaskDistributionList">
            <div class="panel-footer">
              <span class="pull-left">View Task Distrbution</span>
              <span class="pull-right">
                <i class="fa fa-arrow-circle-right"></i>
              </span>
              <div class="clearfix"></div>
            </div>
          </a>
        </div>
      </div>
      <div class="col-lg-3 col-md-6" id="summaryCoreUtilizationPanel">
        <div class="panel panel-green">
          <div class="panel-heading">
            <div class="row">
              <div class="col-xs-3">
                <i class="fa fa-bar-chart-o fa-5x"></i>
              </div>
              <div class="col-xs-9 text-right">
                <div class="huge" id="summaryCoreUtilization">--</div>
                <div>Core Utilization</div>
              </div>
            </div>
          </div>
          <a href="#chartCoreUsageByLocality">
            <div class="panel-footer">
              <span class="pull-left">View Core Usage Time Series</span>
              <span class="pull-right">
                <i class="fa fa-arrow-circle-right"></i>
              </span>
              <div class="clearfix"></div>
            </div>
          </a>
        </div>
      </div>
      <div class="col-lg-3 col-md-6" id="summaryIdleTimePanel">
        <div class="panel panel-yellow">
          <div class="panel-heading">
            <div class="row">
              <div class="col-xs-3">
                <i class="fa fa-spinner fa-5x"></i>
              </div>
              <div class="col-xs-9 text-right">
                <div class="huge" id="summaryIdleTime">--</div>
                <div>Idle Time</div>
              </div>
            </div>
          </div>
          <a href="#chartCoreUsageDistribution">
            <div class="panel-footer">
              <span class="pull-left">View Core Usage Distribution</span>
              <span class="pull-right">
                <i class="fa fa-arrow-circle-right"></i>
              </span>
              <div class="clearfix"></div>
            </div>
          </a>
        </div>
      </div>
    </div>

  def coreUsageDistribution: Seq[Node] =
    <div class="panel panel-default" id="chartCoreUsageDistribution">
      <div class="panel-heading">
        <i class="fa fa-bar-chart fa-fw"></i>
        Core Usage Distribution
      </div>
      <!-- /.panel-heading -->
      <div class="panel-body">
        <div id="core-usage-chart"></div>
      </div>
      <!-- /.panel-body -->
    </div>

  def coreUsageTimeSeriesByLocality: Seq[Node] =
    <div class="panel panel-default" id="chartCoreUsageByLocality">
      <div class="panel-heading">
        <i class="fa fa-line-chart fa-fw"></i>
        Core Usage By Locality
      </div>
      <!-- /.panel-heading -->
      <div class="panel-body">
        <div id="core-usage-line"></div>
      </div>
      <!-- /.panel-body -->
    </div>

  def coreUsageTimeSeriesByPool: Seq[Node] =
    <div class="panel panel-default" id="chartCoreUsageByPool" style="display: none">
      <div class="panel-heading">
        <i class="fa fa-line-chart fa-fw"></i>
        Core Usage By Fair Scheduler Pool
      </div>
      <!-- /.panel-heading -->
      <div class="panel-body">
        <div id="pool-usage-line"></div>
      </div>
      <!-- /.panel-body -->
    </div>

  def taskDistributionList: Seq[Node] =
    <div class="panel panel-default" id="chartTaskDistributionList">
      <div class="panel-heading">
        <i class="fa fa-th fa-fw"></i>
        Task Distribution
      </div>
      <!-- /.panel-heading -->
      <div class="panel-body">
        <div class="list-group" id="task-distribution-list"></div>
      </div>
      <!-- /.panel-body -->
    </div>

  override protected def extraCSS: Seq[Node] = <link rel="stylesheet" type="text/css" href="/static/css/sparklint.css"/>

  override protected def extraScripts: Seq[Node] = Seq(
    <script src="/static/js/sparklintHomepage.js"></script>
  )
}
