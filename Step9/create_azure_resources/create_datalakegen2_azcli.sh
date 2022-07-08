#LOGIN TO AZCLI AND SET SUBSCRIPTION
az login
az account set --subscription <subscription-id>

#CREATE STORAGE ACCOUNT RESOURCE GROUP
az group create \
  --name storageaus_rg \
  --location australiaeast

#CREATE STORAGE ACCOUNT
az storage account create \
    --name pipelinestorageacctaus \
    --resource-group storageaus_rg \
    --location australiaeast \
    --sku Standard_RAGRS \
    --kind StorageV2

#CREATE CONTAINER
az storage fs create -n severeweathercontainer --account-name pipelinestorageacctaus --auth-mode login

#CREATE DIRECTORIES FOR BOTH TABLES
az storage fs directory create -n details -f severeweathercontainer --account-name pipelinestorageacctaus --auth-mode login
az storage fs directory create -n fatalities -f severeweathercontainer --account-name pipelinestorageacctaus --auth-mode login
