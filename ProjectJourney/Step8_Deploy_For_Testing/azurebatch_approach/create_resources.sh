source .secrets
set -eux
az login
az config set extension.use_dynamic_install=yes_prompt

##  AZURE BATCH---------------------------------------------------------------

#SET SUBSCRIPTION
az account set \
  --subscription ${AZ_SUBSCRIPTION_ID}

#CREATE BATCH ACCOUNT RESOURCE GROUP
az group create \
    --name ${AZ_BATCH_RESOURCE_GROUP} \
    --location ${AZ_LOCATION}

#CREATE BATCH STORAGE ACCOUNT
az storage account create --name ${AZ_BATCH_STORAGE_ACCOUNT_NAME} \
  --resource-group ${AZ_BATCH_RESOURCE_GROUP} \
  --location ${AZ_LOCATION} \
  --sku Standard_LRS

#CREATE BATCH ACCOUNT
  az batch account create \
      --name ${AZ_BATCH_ACCOUNT_NAME} \
      --storage-account ${AZ_BATCH_STORAGE_ACCOUNT_NAME} \
      --resource-group ${AZ_BATCH_RESOURCE_GROUP} \
      --location ${AZ_LOCATION} \

#MUST LOGIN TO BATCH ACCOUNT
az batch account login \
    --name ${AZ_BATCH_ACCOUNT_NAME} \
    --resource-group ${AZ_BATCH_RESOURCE_GROUP} \
    --shared-key-auth

#CREATE BATCH POOL
az batch pool create \
    --id ${AZ_BATCH_POOL_ID} --vm-size ${AZ_BATCH_VM_SIZE} \
    --target-dedicated-nodes ${AZ_BATCH_NODE_COUNT}  \
    --image canonical:ubuntuserver:18.04-LTS \
    --node-agent-sku-id "batch.node.ubuntu 18.04"

az batch pool show --pool-id ${AZ_BATCH_POOL_ID} --query "allocationState"
