/// <reference path="typings/underscore/underscore.d.ts" />
/// <reference path="typings/q/Q.d.ts" />
/// <reference path="typings/request/request.d.ts" />

var _ = require('underscore');
var oozie = require('./oozie_api.js');
var fs = require('fs');

var args = process.argv;
// Example (debug)
//var args = ['node', 'app.js', 'https://<cluster_name>.azurehdinsight.net', 'user', 'password', 'status'];

if (args.length < 5) {
    printOptions();
    process.exit();
}

// Get creds
var uri = args[2],
    user = args[3],
    password = args[4],
    option = args[5];

// Create oozie client
var oozieClient = oozie.createOozieClient({
    uri: uri,
    user: user,
    pass: password
});

// Check selected option
switch (option) {

    case 'status':
        getStatus();
        break;

    case 'run':
        if (args.length != 7) {
            printOptions();
            exitApp();
        }

        var filePath = args[6];
        runJob(filePath);
        break;

    case 'check':

        if (args.length != 7) {
            printOptions();
            exitApp();
        }

        var jobId = args[6];
        checkJob(jobId);
        break;

    case 'jobs':

        var status = null;
        if (args.length == 7) {
            status = args[6];
        }
        getJobs(status);
        break;

    case 'log':

        if (args.length != 7) {
            printOptions();
            exitApp();
        }

        var jobId = args[6];
        logJob(jobId);
        break;

    default:
        printOptions();
        exitApp();
        break;
}


// Get Job log
function logJob(jobId) {
    oozieClient.getJobLog(jobId)
        .then(function (jobLog) {
            console.log(jobLog);
            exitApp();
        }, function (err) {
            console.error(err);
        });
}

// Get jobs
function getJobs(status) {

    // Get Jobs 
    oozieClient.getJobs({})
        .then(function (jobs) {
            processJobs(jobs);
        }, function (err) {
            console.error(err);
        });

    function processJobs(jobs) {

        var filteredWorkflows = jobs.workflows;
        if (status != null)
            filteredWorkflows = _.filter(jobs.workflows, function (workflow) {
                return workflow.status.toLowerCase() == status.toLowerCase();
            });                

        filteredWorkflows.forEach(function (wf) {
            console.log(wf.id + ' ' + wf.startTime + ' ' + wf.status);
        }, this);

        exitApp();
    }
}

// Check oozie job
function checkJob(jobId) {

    oozieClient.getJobInfo(jobId)
        .then(function (jobDetails) {

            console.log(jobId + ' ' + jobDetails.status);
            for (var i in jobDetails.actions) {
                var action = jobDetails.actions[i];
                console.log(action.name + ' ' + action.type + ' ' + action.status);
            }

            exitApp();
        }, function (err) {
            console.error(err);
        });
}

// Run oozie job
function runJob(filePath) {

    fs.exists(filePath, function (isExist) {

        fs.readFile(filePath, 'utf8', function (err, data) {
           
            // Job params in json
            var jobParams = JSON.parse(data);
           
            // Run job
            oozieClient.runJob(jobParams)
                .then(function (response) {
                    console.log(response.id);
                    exitApp();
                }, function (err) {
                    console.log(err);
                });
        });
    });
}

// Get oozie status
function getStatus() {

    oozieClient.getStatus()
        .then(function (status) {
            console.log('Oozie status: ' + status.systemMode);
            exitApp();
        }, function (err) {
            console.error(err);
        });
}

// App options
function printOptions() {
    console.log('<user> <pass> [status | run <job.properties> | check <jobId> | jobs <all | running | killed | succeeded | failed>]');
}

// Exit app
function exitApp() {
    process.exit();
}
