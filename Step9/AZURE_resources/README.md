# Microsoft Azure cloud resources:

Deploys via Docker container image:

  -data lake blob storage
  -mysql database
  -databricks
  -data factory
    -creates pipelines based on json configs in DATAFACTORY_pipelines
    -runs init pipeline to ingest all available data at source

Requirement: Docker

1) to start azure cli container, run from local terminal:
`./start.sh`

2) now from inside azure cli container shell, run:
`bash-5.1#./create_resources.sh`

3) login to az by entering given code at https://microsoft.com/devicelogin

json metadata describing created resources will print to stdout (execution_log.txt)
