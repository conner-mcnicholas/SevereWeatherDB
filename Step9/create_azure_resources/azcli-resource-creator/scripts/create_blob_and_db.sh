#CREATE STORAGE ACCOUNT RESOURCE GROUP
pushd $(pwd)
cd "$(dirname "$0")"
source ../vars/.secrets
set -eux

az login

az config set extension.use_dynamic_install=yes_prompt

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
