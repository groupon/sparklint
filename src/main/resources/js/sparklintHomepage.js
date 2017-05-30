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

var eventSocketStream;

function loadApp(esmId, appId) {
    if (eventSocketStream !== undefined) {
        eventSocketStream.close();
    }
    eventSocketStream = new WebSocket("ws://" + window.location.host + "/backend/esm/" + esmId + "/" + appId + "/stateWS");
    eventSocketStream.addEventListener('message', function (statePayload) {
        var data = JSON.parse(statePayload.data);
        displayAppState(appId, data);
        if (data.applicationEndedAt !== undefined && eventSocketStream !== undefined) {
            eventSocketStream.close();
            console.log("close socket stream because application has ended")
        }
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
    var esmId = $(ev.currentTarget).parents(".eventSourceManager").data("esmUuid");
    loadApp(esmId, appId);
}

function inactiveAppClicked(ev) {
    ev.preventDefault();
    var appId = $(ev.currentTarget).data("value");
    var esmId = $(ev.currentTarget).parents(".eventSourceManager").data("esmUuid");
    var url = "/backend/esm/" + esmId + "/" + appId + "/" + "activate";
    $.ajax({
        url: url,
        method: 'POST',
        error: function (exMsg) {
            console.log(exMsg);
            displayErrorMessage(exMsg.responseText);
        }
    }).done(function () {
        refreshEventSourceManagerList(function () {
            loadApp(esmId, appId);
        });
    }).fail(function () {
        refreshEventSourceManagerList();
    });
    // Refresh the list when ready
}

function refreshEventSourceManagerList(onFinish) {
    console.log("refreshEventSourceManagerList");
    var url = "/eventSourceManagerList";
    handleHTML(url, function (responseHtml) {
        var sideMenu = $("#side-menu");
        sideMenu.metisMenu('dispose');
        sideMenu.children(".eventSourceManager").remove();
        sideMenu.append(responseHtml);
        sideMenu.metisMenu();
        sideMenu.find(".inactiveApp").click(inactiveAppClicked);
        sideMenu.find(".sparklintApp").click(appSelectorClicked);
    }).always(onFinish);
}

function displayAppState(appId, appState) {
    updateNameAndId(appState);
    updateSummaryNumExecutors(appState);
    updateSummaryPanel(appState);
    updateSummaryAppDuration(appState);
    updateProgressBarFor($("#summaryApplicationProgress"), appState);
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
    var enabledEventSource = appState.progress.has_next || appState.progress.has_previous;
    if (enabledEventSource) {
        $("#replay-controls").show();
    } else {
        $("#replay-controls").hide();
    }

    // set the correct label and progress bar for the event source
    var labelSelector = "#" + appId + "-app-prog";
    var description = appState.progress.description;
    $(labelSelector).text(description);

    updateProgressBarFor($("#" + appId + "-progress-bar"), appState);

    // set button enabled state on the replay panel
    $("#eventsToStart").prop("disabled", !appState.progress.has_previous);
    $("#eventsBackward").prop("disabled", !appState.progress.has_previous);
    $("#eventsForward").prop("disabled", !appState.progress.has_next);
    $("#eventsToEnd").prop("disabled", !appState.progress.has_next);
}

function updateNameAndId(appState) {
    $("#appName").text(appState.appName || "<unknown>");
    $("#appId").text(appState.appId || "<unknown>");
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
    var summaryApplicationEndedAlert = $("#summaryApplicationEndedAlert");
    if (appState.applicationEndedAt && appState.applicationLaunchedAt) {
        summaryApplicationEndedAlert.addClass("alert-info").removeClass("alert-success");
        summaryTaskPanel.hide();
        chartTaskDistributionList.hide();
    } else {
        summaryApplicationEndedAlert.addClass("alert-success").removeClass("alert-danger");
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

function updateSummaryAppDuration(appState) {
    var summaryApplicationDuration = $("#summaryApplicationDuration");
    var start = moment(appState.applicationLaunchedAt);
    if (appState.applicationEndedAt && appState.applicationLaunchedAt) {
        var end = moment(appState.applicationEndedAt);
        var duration = moment.duration(appState.applicationEndedAt - appState.applicationLaunchedAt);
        summaryApplicationDuration.text("Application finished in " + duration.humanize() +
            " (" + start.toISOString() + " -> " + end.toISOString() + ")");
    } else {
        summaryApplicationDuration.text("Application is still running. (Since " + start.toISOString() + ")");
    }
}

function updateProgressBarFor(progressBar, appState) {
    progressBar.removeClass("progress-bar-success progress-bar-info progress-bar-striped active");
    var percent = appState.progress.percent.toString();
    progressBar.attr('style', 'width: ' + percent + '%').attr('aria-valuenow', percent);
    progressBar.find(".sr-only").text(appState.progress.description);
    if (appState.progress.percent == '100') {
        progressBar.addClass("progress-bar-success");
    } else {
        progressBar.addClass("progress-bar-info active progress-bar-striped");
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
    $("#pool-usage-line").text("");
    if (appState.timeSeriesCoreUsage) {
        Morris.Area({
            element: "core-usage-line",
            data: appState.timeSeriesCoreUsage,
            xkey: 'time',
            ykeys: ['process_local', 'node_local', 'rack_local', 'any', 'no_pref', 'idle'],
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
        if (appState.pools.length > 1) {
            var colorPalatte = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854', '#ffd92f', '#e5c494', '#b3b3b3'];
            $("#chartCoreUsageByPool").show();
            Morris.Area({
                element: "pool-usage-line",
                data: appState.timeSeriesCoreUsage,
                xkey: 'time',
                ykeys: appState.pools,
                labels: appState.pools,
                lineColors: appState.pools.map(function (d, i) {
                    return colorPalatte[i % colorPalatte.length];
                }),
                pointSize: 0,
                lineWidth: 2,
                postUnits: 'cores',
                hideHover: 'auto',
                ymax: appState.maxAllocatedCores,
                ymin: 0,
                resize: true
            });
        } else {
            $("#chartCoreUsageByPool").hide();
        }
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
    var currentTarget = $("#side-menu").find("li.selected");
    var appId = currentTarget.data("value");
    var esmId = currentTarget.parents(".eventSourceManager").data("esmUuid");
    if (!appId || !esmId) return;

    var count = $("#countSelector").val();
    var type = $("#typeSelector").val();
    var url = "/backend/esm/" + esmId + "/" + appId + "/" + direction + "/" + count + "/" + type;
    handleJSON(url, function (progJson) {
        console.log("moved " + appId + " " + direction + " by " + count + " " + type + "(s): " + JSON.stringify(progJson));
    });
    $(".loading-spinner").hide();
}

function moveEventsToEnd(end) {
    var currentTarget = $("#side-menu").find("li.selected");
    var appId = currentTarget.data("value");
    var esmId = currentTarget.parents(".eventSourceManager").data("esmUuid");
    if (!appId || !esmId) return;

    var url = "/backend/esm/" + esmId + "/" + appId + "/to_" + end;
    handleJSON(url, function (progJson) {
        console.log("moved " + appId + " to " + end + ": " + JSON.stringify(progJson));
    });
    $(".loading-spinner").hide();
}

function addSingleFile() {
    var fileName = window.prompt("Provide the uri of the file", "/path/to/file");
    if (fileName !== null) {
        addEventSourceManager("/backend/esm/singleFile", fileName.trim());
    }
    return false;
}

function addDirectory() {
    var folderName = window.prompt("Provide the uri of the folder", "/path/to/folder");
    if (folderName !== null) {
        addEventSourceManager("/backend/esm/folder", folderName.trim());
    }
    return false;
}

function addHistoryServer() {
    var historyServerUri = window.prompt("Provide the uri of the history server", "http://url/to/server");
    if (historyServerUri !== null) {
        addEventSourceManager("/backend/esm/historyServer", historyServerUri);
    }
    return false;
}

function addEventSourceManager(url, data) {
    $.ajax({
        url: url,
        dataType: 'json',
        method: 'POST',
        data: data
    }).done(function (data) {
        hideErrorMessage();
        console.info(data);
    }).fail(function (xhr) {
        console.error(xhr);
        displayErrorMessage(xhr.responseText);
    }).always(function () {
        refreshEventSourceManagerList();
    });
}

function handleJSON(url, successFn) {
    hideErrorMessage();
    return $.ajax({
        url: url,
        dataType: 'json',
        success: successFn,
        error: function (exMsg) {
            console.log(exMsg);
            displayErrorMessage(exMsg.responseText);
        }
    });
}

function handleHTML(url, successFn) {
    return $.ajax({
        url: url,
        dataType: 'html',
        success: successFn,
        error: function (exMsg) {
            console.log(exMsg);
            displayErrorMessage(exMsg.responseText);
        }
    });
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
    $("#eventsToStart").click(eventsToStart);
    $("#eventsToEnd").click(eventsToEnd);
    $("#eventsBackward").click(eventsBackward);
    $("#eventsForward").click(eventsForward);
    $("#addSingleFile").click(addSingleFile);
    $("#addDirectory").click(addDirectory);
    $("#addHistoryServer").click(addHistoryServer);

    console.log("------Setting start state--------");
    $(".loading-spinner").hide();
    hideErrorMessage();

    console.log("------Loading EventSourceManagers------");
    refreshEventSourceManagerList();
});
