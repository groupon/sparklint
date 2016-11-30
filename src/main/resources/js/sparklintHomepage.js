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

function loadApp(appId) {
    var url = "/" + appId + "/state";
    handleJSON(url, function (responseJson) {
        console.log(responseJson);
        displayAppState(appId, responseJson);
    });
    return false; // prevent the url navigate to /# with false as the return value
}

function appSelectorClicked(ev) {
    ev.preventDefault();

    var sideMenu = $("#side-menu");
    sideMenu.find("li").removeClass("selected");
    $(ev.currentTarget).closest('li').addClass('selected');
    sideMenu.find(".sparklintApp .progress-bar").removeClass("progress-bar-info progress-bar-success progress-bar-striped active");

    var appId = $(ev.currentTarget).data("value");
    loadApp(appId);
}

function displayAppState(appId, appState) {
    updateMainHeader(appState);
    updateSummaryMeta(appState);
    updateDetailedProgress(appState.progress);
    updateSummaryNumExecutors(appState);
    updateSummaryPanel(appState);
    updateSummaryCoreUtilization(appState);
    updateIdleTimePanel(appState);
    updateCoreUsageChart(appState);
    updateTaskDistributionList(appState);
    updateEventSourceControl(appId, appState);
}

function displayErrorMessage(errorMsg) {
    var msgDiv = $("#error-message");
    msgDiv.text(errorMsg);
    msgDiv.show();
}

function hideErrorMessage() {
    $("#error-message").hide();
}

function updateEventSourceControl(appId, appState) {
    // optionally enable the event source replay panel
    var eventProgress = appState.progress.events;
    var enabledEventSource = eventProgress.has_next || eventProgress.has_previous;
    if (enabledEventSource) {
        $("#replay-controls").show();
    } else {
        $("#replay-controls").hide();
    }

    // set the correct label and progress bar for the event source
    var labelSelector = "#" + appId + "-app-prog";
    var description = eventProgress.description;
    $(labelSelector).text(description);

    updateProgressBarFor($("#" + appId + "-text"), $("#" + appId + "-progress-bar"), eventProgress, progressDescription(eventProgress));

    // set button enabled state on the replay panel
    $("#eventsToStart").prop("disabled", !eventProgress.has_previous);
    $("#eventsBackward").prop("disabled", !eventProgress.has_previous);
    $("#eventsForward").prop("disabled", !eventProgress.has_next);
    $("#eventsToEnd").prop("disabled", !eventProgress.has_next);
}

function updateMainHeader(appState) {
    $("#appName").text(appState.appName || "<unknown>");
    var idAndDuration = (appState.appId || "<unknown>") + " ".repeat(5) +
        appDuration(appState.applicationLaunchedAt, appState.applicationEndedAt) || "<unknown>";
    $("#appId").text(idAndDuration);
}

function updateSummaryMeta(appState) {
    $("#spark_host").text(fullHostName(appState));
    $("#spark_start").text(moment(appState.applicationLaunchedAt).toISOString());
    var end_time_row = $("#spark_end");
    if (appState.applicationEndedAt) {
        end_time_row.text(moment(appState.applicationEndedAt).toISOString());
    } else {
        end_time_row.text("running");
    }
    $("#summary-meta").show();
}

function updateDetailedProgress(progressTracker) {
    updateDetailedProgressFor("task", progressTracker.tasks, progressDescription(progressTracker.tasks), "");
    updateDetailedProgressFor("stage", progressTracker.stages, progressCounts(progressTracker.stages), inFlightDescription(progressTracker.stages));
    updateDetailedProgressFor("job", progressTracker.jobs, progressCounts(progressTracker.jobs), inFlightDescription(progressTracker.jobs));
    $("#detailed-progress").show();
}

function updateDetailedProgressFor(eventType, progress, description, activeText) {
    updateProgressBarFor($("#" + eventType + "-progress-text"),
        $("#" + eventType + "-progress-bar"), progress, description);
    $("#" + eventType + "-progress-active").text(activeText);
}

function updateSummaryNumExecutors(appState) {
    $("#summaryRow").show();
    var summaryNumExecutors = $("#summaryNumExecutors");
    if (appState.executors) {
        summaryNumExecutors.text(appState.executors.length);
    } else {
        summaryNumExecutors.text(0);
    }
}

function updateSummaryPanel(appState) {
    var summaryTaskPanel = $("#summaryTaskPanel");
    var chartTaskDistributionList = $("#chartTaskDistributionList");
    if (appState.applicationEndedAt && appState.applicationLaunchedAt) {
        summaryTaskPanel.hide();
        chartTaskDistributionList.hide();
    } else {
        summaryTaskPanel.show();
        chartTaskDistributionList.show();

        var summaryNumTasks = $("#summaryNumTasks");
        if (appState.runningTasks) {
            summaryNumTasks.text(appState.runningTasks);
        } else {
            summaryNumTasks.text(0);
        }
    }
}

function updateSummaryCoreUtilization(appState) {
    var summaryCoreUtilization = $("#summaryCoreUtilization");
    if (appState.coreUtilizationPercentage) {
        summaryCoreUtilization.text((appState.coreUtilizationPercentage * 100).toFixed(2) + "%");
    } else {
        summaryCoreUtilization.text("0.0%");
    }
}

function updateProgressBarFor(progressTextDiv, progressBarDiv, eventProgress, progressText) {
    progressBarDiv.removeClass("progress-bar-success progress-bar-info progress-bar-striped active");
    var percent = eventProgress.percent.toString();
    topLevelText(progressTextDiv, progressText);
    progressBarDiv.attr('style', 'width: ' + percent + '%').attr('aria-valuenow', percent);
    if (eventProgress.percent == '100') {
        progressBarDiv.addClass("progress-bar-success");
    } else {
        progressBarDiv.addClass("progress-bar-info active progress-bar-striped");
    }
}

function updateCoreUsageChart(appState) {
    $("#core-usage-chart").text("");
    if (appState.cumulativeCoreUsage) {
        Morris.Bar({
            element: "core-usage-chart",
            data: appState.cumulativeCoreUsage,
            xkey: 'cores',
            ykeys: ['duration'],
            labels: ['duration (ms)'],
            hideHover: 'auto',
            resize: true
        });
    }
    $("#core-usage-line").text("");
    if (appState.timeSeriesCoreUsage) {
        Morris.Area({
            element: "core-usage-line",
            data: appState.timeSeriesCoreUsage,
            xkey: 'time',
            ykeys: ['processLocal', 'nodeLocal', 'rackLocal', 'any', 'noPref', 'idle'],
            labels: ['PROCESS_LOCAL', 'NODE_LOCAL', 'RACK_LOCAL', 'ANY', 'NO_PREF', 'Idle'],
            lineColors: ['#27ae60', '#f1c40f', '#e67e22', '#e74c3c', '#34495e', '#95a5a6'],
            pointSize: 0,
            lineWidth: 2,
            postUnits: 'cores',
            hideHover: 'auto',
            ymax: appState.maxAllocatedCores,
            ymin: 0,
            resize: true
        });
    }
}

function updateIdleTimePanel(appState) {
    var summaryIdleTime = $("#summaryIdleTime");
    if (appState.idleTimeSinceFirstTask) {
        var idleTime = appState.idleTimeSinceFirstTask;
        var appDuration = (appState.finishedAt || appState.lastUpdatedAt) - appState.applicationLaunchedAt;
        if (appDuration) {
            summaryIdleTime.text((idleTime / appDuration * 100).toFixed(2) + "%");
        } else {
            summaryIdleTime.text("0%");
        }
    } else {
        summaryIdleTime.text("0%");
    }
}

function updateTaskDistributionList(appState) {
    var taskDistributionList = $("#task-distribution-list");
    taskDistributionList.text("");
    if (appState.currentTaskByExecutor && appState.executors) {
        var tasksLookup = _.indexBy(appState.currentTaskByExecutor, "executorId");
        _.chain(appState.executors).filter(function (executorInfo) {
            return !_.has(executorInfo, "end")
        }).sortBy(function (executorInfo) {
            return executorInfo.executorId;
        }).value().forEach(function (executorInfo) {
            var tasksByExecutor = tasksLookup[executorInfo.executorId];
            taskDistributionList.append(drawExecutor(executorInfo, tasksByExecutor ? tasksByExecutor.tasks : []));
        });
    }
}

function drawExecutor(executorInfo, tasks) {
    var cores = executorInfo.cores;
    var root = $(document.createElement('div')).addClass("row");
    var executorLabel = $(document.createElement('div')).addClass('col-lg-2 col-md-4 col-sm-6').text("Executor" + executorInfo.executorId);
    root.append(executorLabel);
    var taskList = $(document.createElement('div')).addClass('row');

    tasks.forEach(function (task, index) {
        var taskButton = $(document.createElement('div')).addClass("col-lg-2 col-md-3 col-sm-6 btn").text(task.taskId);
        taskButton.addClass(index >= cores ? "btn-warning" : "btn-success");
        taskList.append(taskButton);
    });
    for (var i = tasks.length; i < cores; i++) {
        var taskButton = $(document.createElement('div')).addClass("col-lg-2 col-md-3 col-sm-6 btn btn-default").text("idle");
        taskList.append(taskButton);
    }
    root.append($(document.createElement('div')).addClass('col-lg-10 col-md-8 col-sm-6').append(taskList));
    return $(document.createElement('div')).addClass('list-group-item').append(root);
}

function fullHostName(appName) {
    return appName.user + "@" + appName.host + ":" + appName.port.toString();
}

function appDuration(start, end) {
    if (end) {
        return "finished in " + moment.duration(end - start).humanize();
    } else {
        // streaming, assume wall clock time is end time.
        return "running for " + moment.duration(moment() - moment(start)).humanize();
    }
}

function progressDescription(progress) {
    return progress.complete.toString() + " / " + progress.count.toString() + " (" + progress.percent.toString() + "%)";
}

function progressCounts(progress) {
    return progress.complete.toString() + " / " + progress.count.toString() + " (" + progress.in_flight.toString() + " active)";
}

function inFlightDescription(progress) {
    if (progress.in_flight > 0) {
        return "Active: " + progress.active.join();
    } else {
        return "";
    }
}

// ####### EventSource progression controls #######

function eventsToStart() {
    moveEventsToEnd("start")
}

function eventsToEnd() {
    moveEventsToEnd("end")
}

function eventsBackward() {
    moveEvents("rewind")
}

function eventsForward() {
    moveEvents("forward")
}

function moveEvents(direction) {
    var appId = $("#side-menu").find("li.selected").data("value");
    if (!appId) return;

    var count = $("#countSelector").val();
    var type = $("#typeSelector").val();
    var url = "/" + appId + "/" + direction + "/" + count + "/" + type;
    handleJSON(url, function (progJson) {
        console.log("moved " + appId + " " + direction + " by " + count + " " + type + "(s): " + JSON.stringify(progJson.events));
        loadApp(appId);
    })
}

function moveEventsToEnd(end) {
    var appId = $("#side-menu").find("li.selected").data("value");
    if (!appId) return;

    var url = "/" + appId + "/to_" + end;
    handleJSON(url, function (progJson) {
        console.log("moved " + appId + " to " + end + ": " + JSON.stringify(progJson.events));
        loadApp(appId);
    });
}

function handleJSON(url, successFn) {
    hideErrorMessage();
    $.ajax({
        url: url,
        dataType: 'json',
        success: successFn,
        error: function (exMsg) {
            console.log(exMsg);
            displayErrorMessage(exMsg.responseText);
        }
    });
}

function topLevelText(node, textValue) {
    var children = node.children();
    node.text(textValue).prepend(children);
}

$(function () {
    console.log("------Global document binding------");
    $(document).ajaxStop(function () {
        console.debug("ajaxStop");
        $(".loading-spinner").hide();
    });
    $(document).ajaxStart(function () {
        console.debug("ajaxStart");
        $(".loading-spinner").show();
    });

    console.log("------Sparklint control binding------");
    $("#side-menu").find(".sparklintApp").click(appSelectorClicked);
    $("#eventsToStart").click(eventsToStart);
    $("#eventsToEnd").click(eventsToEnd);
    $("#eventsBackward").click(eventsBackward);
    $("#eventsForward").click(eventsForward);

    console.log("------Setting start state--------");
    $(".loading-spinner").hide();
    hideErrorMessage();
});