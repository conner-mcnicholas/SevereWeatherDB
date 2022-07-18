source .secrets
set -eux
az login
az config set extension.use_dynamic_install=yes_prompt

##  AZURE DATA LAKE GEN2---------------------------------------------------------------------------

#CREATE STORAGE ACCOUNT RESOURCE GROUP
az account set \
  --subscription ${AZ_SUBSCRIPTION_ID}

az group create \
  --name ${AZ_STORAGE_RESOURCE_GROUP} \
  --location ${AZ_LOCATION}

#CREATE STORAGE ACCOUNT
az storage account create \
    --name ${AZ_STORAGE_ACCOUNT_NAME} \
    --resource-group ${AZ_STORAGE_RESOURCE_GROUP} \
    --location ${AZ_LOCATION} \
    --sku Standard_RAGRS \
    --kind StorageV2

#MUST UPGRADE FROM BLOB STORAGE TO AZURE DATA LAKE GEN2
az storage account hns-migration start --type validation -n ${AZ_STORAGE_ACCOUNT_NAME} -g ${AZ_STORAGE_RESOURCE_GROUP}
az storage account hns-migration start --type upgrade -n ${AZ_STORAGE_ACCOUNT_NAME} -g ${AZ_STORAGE_RESOURCE_GROUP}

#CREATE CONTAINER + TABLE SUBDIRS FOR INITIAL BATCH LOADING OF ALL FILES SINCE START YEAR
az storage fs create \
  -n ${AZ_BATCH_CONTAINER_NAME} \
  --account-name ${AZ_STORAGE_ACCOUNT_NAME} \
  --auth-mode login

az storage fs directory create \
  -f ${AZ_BATCH_CONTAINER_NAME} \
  -n details \
  --account-name ${AZ_STORAGE_ACCOUNT_NAME} \
  --auth-mode key

az storage fs directory create \
  -f ${AZ_BATCH_CONTAINER_NAME} \
  -n fatalities \
  --account-name ${AZ_STORAGE_ACCOUNT_NAME} \
  --auth-mode key

#CREATE CONTAINER + TABLE SUBDIRS FOR INCREMENTAL LOAD OF UPDATED/NEW FILES
az storage fs create \
  -n ${AZ_NEWFILES_CONTAINER_NAME}\
  --account-name ${AZ_STORAGE_ACCOUNT_NAME} \
  --auth-mode login

az storage fs directory create \
  -f ${AZ_NEWFILES_CONTAINER_NAME} \
  -n details \
  --account-name ${AZ_STORAGE_ACCOUNT_NAME} \
  --auth-mode key

  az storage fs directory create \
    -f ${AZ_NEWFILES_CONTAINER_NAME} \
    -n fatalities \
    --account-name ${AZ_STORAGE_ACCOUNT_NAME} \
    --auth-mode key

## MYSQL DB---------------------------------------------------------------------------

#Create an Azure resource group using the az group create command and then create your MySQL server inside this resource group.
az group create \
  --name ${AZ_MYSQL_RESOURCE_GROUP} \
  --location ${AZ_LOCATION}

#Create an Azure Database for MySQL server with the az mysql server create command
az mysql server create \
  --resource-group ${AZ_MYSQL_RESOURCE_GROUP} \
  --name ${AZ_MYSQL_SERVER_NAME} \
  --location ${AZ_LOCATION} \
  --admin-user ${AZ_MYSQL_ADMIN} \
  --admin-password ${AZ_MYSQL_ADMIN_PASSWORD} \
  --sku-name GP_Gen5_2

#Configure the firewall rule on your  IP and all azure services (0.0.0.0) using the az mysql server firewall-rule create command.
az mysql server firewall-rule create \
  --resource-group ${AZ_MYSQL_RESOURCE_GROUP} \
  --server ${AZ_MYSQL_SERVER_NAME} \
  --name AllowMyIP \
  --start-ip-address ${AZ_MY_IP} \
  --end-ip-address ${AZ_MY_IP}
az mysql server firewall-rule create \
  --resource-group ${AZ_MYSQL_RESOURCE_GROUP} \
  --server ${AZ_MYSQL_SERVER_NAME} \
  --name AllowAzureIP \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

## DATABRICKS---------------------------------------------------------------------------

# Create Resource Group if not exists
az group create --name $AZ_DATABRICKS_RESOURCE_GROUP --location $AZ_LOCATION

# Create databricks workspace
az databricks workspace create \
  --location $AZ_LOCATION \
  --name $AZ_DATABRICKS_WORKSPACE \
  --sku trial \
  --resource-group $AZ_DATABRICKS_RESOURCE_GROUP \
  --enable-no-public-ip \
  --tags environment=demo level=level3

## DATA FACTORY---------------------------------------------------------------------------

#create data factory
az datafactory create --resource-group $AZ_DATAFACTORY_RESOURCE_GROUP \
    --factory-name $AZ_DATAFACTORY_NAME

#create init pipelines
az datafactory pipeline create --resource-group $AZ_DATAFACTORY_RESOURCE_GROUP \
    --factory-name $AZ_DATAFACTORY_NAME --name init_pipeline \
    --pipeline DATAFACTORY_pipelines/init_pipeline/pipeline/init_pipeline.json

#create update pipelines
az datafactory pipeline create --resource-group $AZ_DATAFACTORY_RESOURCE_GROUP \
    --factory-name $AZ_DATAFACTORY_NAME --name update_pipeline \
    --pipeline DATAFACTORY_pipelines/update_pipeline/pipeline/update_pipeline.json

#create new pipelines
az datafactory pipeline create --resource-group $AZ_DATAFACTORY_RESOURCE_GROUP \
    --factory-name $AZ_DATAFACTORY_NAME --name new_pipeline \
    --pipeline DATAFACTORY_pipelines/new_pipeline/pipeline/new_pipeline.json

#run init pipeline
az datafactory pipeline create-run --resource-group $AZ_DATAFACTORY_RESOURCE_GROUP \
    --name init_pipeline --factory-name $AZ_DATAFACTORY_NAME

#verify pipeline run success
az datafactory pipeline-run show --resource-group $AZ_DATAFACTORY_RESOURCE_GROUP \
    --factory-name $AZ_DATAFACTORY_NAME --run-id 00000000-0000-0000-0000-000000000000
