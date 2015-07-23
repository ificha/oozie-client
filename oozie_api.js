
var request = require('request');
var q = require('q');
var _ = require('underscore');

module.exports = {

	createOozieClient: function (options) {
		var _options = options;

		return {
			// Get oozie status
			// return {systemMode: ''}
			getStatus: getStatus,
			
			// Get oozie jobs
			// return: {
			// total: 0,
			// workflows: [
			// { status: '', user: '', createdTime: '', endTime: '', actions: [] }
			//] 
			// }
			getJobs: getJobs,
			
			// Run oozie job (params json)
			// return: {id: ''}
			runJob: runJob,
			
			// Get oozie job detailed info
			getJobInfo: getJobInfo,
			
			// Get oozie job log
			getJobLog: getJobLog
		}

		function getJobLog(jobId) {

			var uri = _options.uri + '/oozie/v1/job/' + jobId + '?show=log';
			return _get(uri);
		}

		function getJobInfo(jobId) {

			var uri = _options.uri + '/oozie/v2/job/' + jobId;
			return _get(uri)
				.then(function (response) {
					return JSON.parse(response);
				}, function (err) {
					return err;
				});
		}

		function runJob(jobParamsJson) {

			var uri = _options.uri + '/oozie/v2/jobs?action=start';

			var xmlParams = _.reduce(_.keys(jobParamsJson), function (context, key) {
				return context + '<property><name>' + key + '</name><value>' + jobParamsJson[key] + '</value></property>';
			}, '');
			var oozieParamsXml = '<?xml version="1.0" encoding="UTF-8"?><configuration>' + xmlParams + '</configuration>';

			return _post(uri, oozieParamsXml, false)
				.then(function(response){
					return JSON.parse(response);
				}, function(err){
					return err;
				});
		}

		function getJobs(params) {

			var uri = _options.uri + '/oozie/v2/jobs';
			return _get(uri)
				.then(function (response) {
					return JSON.parse(response);
				}, function (err) {
					return err;
				});
		}

		function getStatus() {

			var uri = _options.uri + '/oozie/v2/admin/status';
			return _get(uri)
				.then(function (body) {
					return JSON.parse(body);
				}, function (err) {
					return err;
				});
		}

		function _get(uri) {

			var deferred = q.defer();

			request.get(uri, {
				'auth': {
					'user': _options.user,
					'pass': _options.pass,
					'sendImmediately': false
				}
			}, responseFunc);

			function responseFunc(error, response, body) {
				if (!error && response.statusCode == 200) {
					deferred.resolve(body);
				} else {
					deferred.reject(error);
				}
			}

			return deferred.promise;
		}

		function _post(uri, data, isJson) {
			
			var deferred = q.defer();

			request({
				uri: uri,
				method: 'POST',
				headers: {
					"content-type": "application/xml",
					"content-length": Buffer.byteLength(data)
				},
				auth: {
					'user': _options.user,
					'pass': _options.pass,
					'sendImmediately': false
				},
				json: isJson,
				body: data
			}, responseFunc);

			function responseFunc(error, response, body) {

				if (!error && response.statusCode == 201) {					
					deferred.resolve(body);
				} else {
					deferred.reject(error);
				}
			}

			return deferred.promise;			
		}


	}
};


