/// <reference path="typings/underscore/underscore.d.ts" />
/// <reference path="typings/q/Q.d.ts" />
/// <reference path="typings/request/request.d.ts" />

var _ = require('underscore');
var oozie = require('./oozie_api.js');
var fs = require('fs');
var q = require('q');
var argumentParser = require("node-argument-parser");

var _configFileName = './config.json';

// enter point
main();

function main() {
    parseArgs()
        .then(function (args) {

            processOptions(args);

        }, function (err) {
            console.log(err);
        });
}

// Parse args from command line && merge with saved args from _configFileName
function parseArgs() {

    var deffered = q.defer();
    
    // Parse args
    var args = argumentParser.parse('./arguments.json', process);
    
    // Save args into config.json
    if (args.save) {
        args.help = undefined;
        args.save = undefined;
        var argsStr = JSON.stringify(args);
        fs.writeFile(_configFileName, argsStr, function (err) {
            if (err) {
                console.log(err);
                deffered.reject(err);
            }

            console.log('params were saved to config.json');
            deffered.resolve(args);
        });
    } else {

        if (fs.existsSync(_configFileName)) {
            
            // Read params from config.json and combine
            fs.readFile(_configFileName, 'utf8', function (err, data) {

                if (err) {
                    deffered.reject(err);
                }

                var savedArgs = JSON.parse(data);
                _.each(_.keys(savedArgs), function (key) {

                    var savedVal = savedArgs[key];
                    if (args[key] == undefined && savedVal != undefined)
                        args[key] = savedVal;
                });

                deffered.resolve(args);
            });
        } else {
            deffered.resolve(args);
        }
    }

    return deffered.promise;
}


function processOptions(args) {

    if (!args.cluster || !args.user || !args.pass)
        return console.log('cluster name, user and pass are required!');

    if (!args.option)
        return console.log('option is required');

    // Create oozie client
    var oozieClient = oozie.createOozieClient({
        uri: _getClusterUri(args.cluster),
        user: args.user,
        pass: args.pass
    });

    // Get last arg (can be jobid, paramFile)
    var cmdArgsAll = process.argv;
    var lastArg = cmdArgsAll[cmdArgsAll.length - 1];

    switch (args.option) {
        case 'status':
            getStatus(oozieClient);                
            break;

        case 'jobs':
            getJobs(oozieClient, lastArg);
            break;

        case 'job':
            checkJob(oozieClient, lastArg);
            break;

        case 'run':
            runJob(oozieClient, lastArg);
            break;

        case 'log':
            logJob(oozieClient, lastArg);
            break;

        default:
            console.log('unknow option');
            break;
    }
}

// Get Job log
function logJob(oozieClient, jobId) {
    oozieClient.getJobLog(jobId)
        .then(function (jobLog) {
            console.log(jobLog);            
        }, function (err) {
            console.error(err);
        });
}

// Get jobs
function getJobs(oozieClient, status) {

    // Get Jobs 
    oozieClient.getJobs({})
        .then(function (jobs) {
            processJobs(jobs);
        }, function (err) {
            console.error(err);
        });

    function processJobs(jobs) {

        var knowStatus = ['succeeded', 'killed', 'failed', 'running', 'suspended']
            .indexOf(status.toLowerCase()) != -1;

        var filteredWorkflows = jobs.workflows;
        if (knowStatus)
            filteredWorkflows = _.filter(jobs.workflows, function (workflow) {
                return workflow.status.toLowerCase() == status.toLowerCase();
            });

        filteredWorkflows.forEach(function (wf) {
            console.log(wf.id + ' ' + wf.startTime + ' ' + wf.status);
        }, this);
        
    }
}

// Check oozie job
function checkJob(oozieClient, jobId) {

    oozieClient.getJobInfo(jobId)
        .then(function (jobDetails) {

            console.log(jobId + ' ' + jobDetails.status);
            for (var i in jobDetails.actions) {
                var action = jobDetails.actions[i];
                console.log(action.name + ' ' + action.type + ' ' + action.status);
            }
            
        }, function (err) {
            console.error(err);
        });
}

// Run oozie job
function runJob(oozieClient, filePath) {

    fs.exists(filePath, function (isExist) {

        fs.readFile(filePath, 'utf8', function (err, data) {
           
            // Job params in json
            var jobParams = JSON.parse(data);
           
            // Run job
            oozieClient.runJob(jobParams)
                .then(function (response) {
                    console.log(response.id);                    
                }, function (err) {
                    console.log(err);
                });
        });
    });
}

// Get oozie status
function getStatus(oozieClient) {

     oozieClient.getStatus()
        .then(function (status) {
            console.log('Oozie status: ' + status.systemMode);            
        }, function (err) {
            console.error(err);
        });
}

function _getClusterUri(name) {
    return 'https://' + name + '.azurehdinsight.net';
}
