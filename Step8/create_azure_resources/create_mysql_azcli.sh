#Start a Docker container with Azure CLI pre-installed
docker run -it mcr.microsoft.com/azure-cli

#(all subsequent commands to be executed from within docker container)

#sign in to the Azure CLI by using the az login command
az login

#When you're prompted, install the Azure CLI extension on first use.
az config set extension.use_dynamic_install=yes_prompt

#Select the specific subscription under your account using az account set command.
az account set --subscription <subscription id>

#Create an Azure resource group using the az group create command and then create your MySQL server inside this resource group.
az group create --name <mysql-resourcegroup> --location australiaeast

#Create an Azure Database for MySQL server with the az mysql server create command
az mysql server create --resource-group <mysql-resourcegroup> --name <mysql-server-name> --location australiaeast --admin-user <adminuser> --admin-password <password> --sku-name GP_Gen5_2

#Configure the firewall rule on your  IP and all azure services (0.0.0.0) using the az mysql server firewall-rule create command.
az mysql server firewall-rule create --resource-group <mysql-resourcegroup> --server <mysql-server-name> --name AllowMyIP --start-ip-address <my-ip-addr> --end-ip-address <my-ip-addr>
az mysql server firewall-rule create --resource-group <mysql-resourcegroup> --server <mysql-server-name> --name AllowMyIP --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
