from datetime import datetime,date,timedelta
import os
import sys
from pathlib import Path
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.storage.blob import ResourceTypes, AccountSasPermissions
from azure.storage.blob import generate_account_sas
import mysql.connector
from mysql.connector import errorcode

def cleanup_containers():
    CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

    client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    # Create sas token for blob
    sas_token = generate_account_sas(
        account_name = client.account_name,
        account_key = <account_key>, # The account key for the source container
        resource_types = ResourceTypes(object=True, container=True),
        permission= AccountSasPermissions(read=True,list=True),
        start = datetime.now(),
        expiry = datetime.utcnow() + timedelta(hours=4) # Token valid for 4 hours
    )

    source_container_name = 'newfiles'
    source_container_client = client.get_container_client(source_container_name)

    destination_container_name = 'testbatch'
    destination_container_client = client.get_container_client(destination_container_name)

    for table in ['details','fatalities']:
        source_container_list = source_container_client.list_blobs()
        for s in source_container_list:
            depth = len(Path(s.name).parts)
            if depth == 2:
                filetype = Path(s.name).parts[0]
                filename = Path(s.name).parts[-1]
                felements = filename.split("_")
                fyear = int(felements[3][1:5])
                if filetype == table:
                    newfile = filename
                    updateyear = fyear
                    new_blob_name = s.name
                    destination_container_list = destination_container_client.list_blobs()
                    for d in destination_container_list:
                        depth = len(Path(d.name).parts)
                        if depth == 2:
                            filetype = Path(d.name).parts[0]
                            filename = Path(d.name).parts[-1]
                            felements = filename.split("_")
                            pyear = int(felements[3][1:5])
                            if filetype == table and pyear == updateyear:
                                print(f'{table} - DELETING old file: {d.name} FROM destination: {d.container}')
                                destination_blob = destination_container_client.get_blob_client(d)
                                destination_blob.delete_blob()

                                new_blob = client.get_blob_client(destination_container_name, new_blob_name)
                                # Create blob client for source blob
                                source_blob = BlobClient(
                                    client.url,
                                    container_name = source_container_name,
                                    blob_name = new_blob_name,
                                    credential = sas_token
                                )
                                print(f'{table} - COPYING new file: {s.name} FROM source: {s.container} TO destination: {d.container}')
                                new_blob.start_copy_from_url(source_blob.url)

                                print(f'{table} - DELETING new file: {s.name} FROM source: {s.container}')
                                source_blob_dl = source_container_client.get_blob_client(s)
                                source_blob_dl.delete_blob()

def create_table_postcounts():
    config = {
      'host':'sevwethmysqlserv.mysql.database.azure.com',
      'user':'conner@sevwethmysqlserv',
      'password':'<password>',
      'database':'defaultdb',
      'client_flags': [mysql.connector.ClientFlag.SSL],
      'ssl_ca': f'{os.environ["HOME"]}/.ssh/DigiCertGlobalRootG2.crt.pem',
      'autocommit': True
    }

    query = ("DROP TABLE IF EXISTS vPostUpdate;"
        "CREATE TABLE vPostUpdate AS"
        "  SELECT d_PostUpdate,f_PostUpdate FROM"
        "	(SELECT COUNT(*) AS  d_PostUpdate  FROM test_details) AS d,"
        "	(SELECT COUNT(*) AS  f_PostUpdate  FROM test_fatalities) AS f;")

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(query)

cleanup_containers()

create_table_postcounts()
