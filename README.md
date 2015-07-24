# oozie-client

# Install
npm install oozie-client

# Get help
node app.js --help

Options:

  -s, --save	save params
  -undefined, --cluster	hdinsight cluster name (expects value)
  -undefined, --user	user (expects value)
  -undefined, --pass	password (expects value)
  -o, --option	options [status | jobs <status> | job <job-id> | run <job.properties> | log <job-id>] (expects value)

# Save cluster params (in config.json)
node app.js --cluster <name> --user <admin> --pass <password> --save 

# Get oozie status
node app.js -o status

# Get jobs which <oozie_status> 
node app.js -o jobs <oozie_status>

# Get job details (with actions)
node app.js -o job <job-id>

# Run job
node app.js -o run <job.properties file>

where job.properties (example):

{

     "nameNode": "wasb://<container_name>@<storage>.blob.core.windows.net",
     "jobTracker": "jobtrackerhost:9010",
     "queueName": "default",
     
     "user.name": "god",
     # container where oozie workflow.xml is
     "oozie.wf.application.path": "wasb://<container>@<storage>.blob.core.windows.net/",
     "outputDir": "ooziejobs-out",
     "oozie.use.system.libpath": "true"          
}

# Get job log
node app.js -o log <job-id>

