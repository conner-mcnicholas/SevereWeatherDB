# Containerized package for deploying Microsoft Azure cloud resources:

data lake blob storage
mysql database
batch job
data factory?

to start azure cli container, run from local terminal:
`./start.sh`

now from inside azure cli container shell, run:
`bash-5.1#./create_resources.sh`

login to az by entering given code at https://microsoft.com/devicelogin

json metadata describing created resources will print to stdout (execution_log.txt)
