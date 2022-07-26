# Microsoft Azure cloud resources:

Deploys via Docker container image:

&emsp;&emsp;-data lake blob storage <br>
&emsp;&emsp;-mysql database <br>
&emsp;&emsp;-databricks <br>
&emsp;&emsp;-data factory <br>
&emsp;&emsp;&emsp;&emsp;-creates pipelines based on json configs in DATAFACTORY_pipelines <br>
&emsp;&emsp;&emsp;&emsp;-runs init pipeline to ingest all available data at source <br>

Requirement: Docker

1) to start azure cli container, run from local terminal:
`./start.sh`

2) now from inside azure cli container shell, run: <br>
`bash-5.1#./create_resources.sh`

3) login to az by entering given code at https://microsoft.com/devicelogin <br>

json metadata describing created resources will print to stdout (execution_log.txt)
