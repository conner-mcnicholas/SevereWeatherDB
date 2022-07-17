import os
import sys
from time import sleep
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import requests
from datetime import datetime,timedelta
from pathlib import Path
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
from azure.core.exceptions import ResourceExistsError
import config
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas
)
import azure.storage.blob as azureblob


from os import listdir
try:
    input = raw_input
except NameError:
    pass

def listall():
    """
    Parses source url text into list of all filenames available to upload

    :return: list of all files hosted at url
    """
    print("\n\n***** Start of method listfull *****\n")
    print("Connecting to url...")
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    print("\nReturning parsed text....")
    return [url + "/" + node.get("href")
            for node in soup.find_all("a") if
            ((not node.get("href").startswith("StormEvents_locations")) and (node.get("href").endswith("csv.gz")))]


def listtarget(flist, start_year):
    """
    Create list of files that are past start date and target for upload to blob

    :param flist: list of all files available at source url.
    :param start_year: earliest year to retrieve data after.
    :return: list of details and fatalities csv.gzs >= start_year
    """
    print("\n\n***** Start of method listtarget *****\n")
    targeted = []
    for file in flist:
        filename = Path(file).parts[-1]
        print(f"Assessing: {filename}")
        felements = filename.split("_")
        ftype = felements[1].split("-")[0]
        fyear = felements[3][1:5]
        if int(fyear) >= start_year:
            print(f"File is past start date({start_year}), adding...\n")
            targeted.append(filename)
        else:
            print(f"File is before start date({start_year}), skipping...\n")
    print("number of target files for loading to blob:" + str(len(targeted)))
    return targeted

def upload_file_to_container(blob_storage_service_client: BlobServiceClient,
                             container: str, blob_name: str) -> batchmodels.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    sourcefile = url+'/'+blob_name
    filename = Path(blob_name).parts[-1]
    filetype = filename.split("_")[1].split("-")[0]
    print(f'Uploading file {blob_name} to container {container}/{filetype}...')
    blob_client = blob_storage_service_client.get_blob_client(f'{container}/{filetype}', blob_name)
    blob_client.start_copy_from_url(sourcefile)

    sas_token = generate_blob_sas(
        config.STORAGE_ACCOUNT_NAME,
        container,
        blob_name,
        account_key=config.STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=2)
    )

    sas_url = generate_sas_url(
        config.STORAGE_ACCOUNT_NAME,
        config.STORAGE_ACCOUNT_DOMAIN,
        container,
        blob_name,
        sas_token
    )

    return batchmodels.ResourceFile(
        http_url=sas_url,
        file_path=blob_name
    )

def generate_sas_url(
    account_name: str,
    account_domain: str,
    container_name: str,
    blob_name: str,
    sas_token: str
) -> str:
    """
    Generates and returns a sas url for accessing blob storage
    """
    return f"https://{account_name}.{account_domain}/{container_name}/{blob_name}?{sas_token}"

def create_pool(batch_service_client: BatchServiceClient, pool_id: str):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print(f'Creating pool [{pool_id}]...')

    # Specify the commands for the pool's start task. The start task is run
    # on each node as it joins the pool, and when it's rebooted or re-imaged.
    # We use the start task to prep the node for running our task script.
    #task_commands = ['apt-get install python3-pip','pip install -r requirements.txt']
    #task_commands = ["curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python","python -m pip install -r requirements.txt"]
    #task_commands = ['echo hi!']
    user = batchmodels.AutoUserSpecification(
        scope=batchmodels.AutoUserScope.pool,
        elevation_level=batchmodels.ElevationLevel.admin)

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="canonical",
                offer="0001-com-ubuntu-server-focal",
                sku="20_04-lts",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 20.04"),
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT,
        start_task=batchmodels.StartTask(
            command_line='/bin/bash -c "sudo apt-get -y update && sudo dpkg --configure -a && sudo apt-get install -y python3-pip && pip3 install --upgrade pip && sudo pip3 install azure-batch==11.0.0"',
            user_identity=batchmodels.UserIdentity(auto_user=user),
            wait_for_success=True)
    )

    batch_service_client.pool.add(new_pool)

def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print(f'Creating job [{job_id}]...')

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_tasks(batch_service_client: BatchServiceClient, job_id: str, resource_input_files: list):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID of the job to which to add the tasks.
    :param list resource_input_files: A collection of input files. One task will be
     created for each input file.
    """
    print(f'Adding {resource_input_files} tasks to job [{job_id}]...')

    tasks = []

    for idx, input_file in enumerate(resource_input_files):

        command = f"/bin/bash -c \"du -h {input_file.file_path}\""
        tasks.append(batchmodels.TaskAddParameter(
            id=f'Task{idx}',
            command_line=command,
            resource_files=[input_file]
        )
        )

    batch_service_client.task.add_collection(job_id, tasks)

def wait_for_tasks_to_complete(batch_service_client: BatchServiceClient, job_id: str,
                               timeout: timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :param job_id: The id of the job whose tasks should be to monitored.
    :param timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True

        sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))

def print_task_output(batch_service_client: BatchServiceClient, job_id: str,
                      text_encoding: str=None):
    """
    Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :param str job_id: The id of the job with task output files to print.
    """

    print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(
            job_id, task.id).node_info.node_id
        print(f"Task: {task.id} --> Node: {node_id}")

def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if (batch_exception.error and batch_exception.error.message and
            batch_exception.error.message.value):
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')

if __name__ == '__main__':
    start_time = datetime.now().replace(microsecond=0)
    print(f'Batch Load Start Time: {start_time}\n')

    start_year = int(sys.argv[1])
    url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    print(f"Orchestrating batch load task for each year after {start_year}...\n")

    CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    input_container_name = "testbatch"

    allfiles = listall()
    # The collection of data files that are to be processed by the tasks.
    input_file_paths = listtarget(allfiles, start_year)

    # Upload the data files.
    input_files = [
        upload_file_to_container(blob_service_client, input_container_name, file_path)
        for file_path in input_file_paths]

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
        config.BATCH_ACCOUNT_KEY)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=config.BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, config.POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, config.JOB_ID, config.POOL_ID)

        # Add the tasks to the job.
        add_tasks(batch_client, config.JOB_ID, input_files)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,
                                   config.JOB_ID,
                                   timedelta(minutes=10))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        print_task_output(batch_client, config.JOB_ID)

        # Print out some timing info
        end_time = datetime.now().replace(microsecond=0)
        print()
        print(f'Batch Load End Time: {end_time}')
        elapsed_time = end_time - start_time
        print(f'Number Of Files Loaded: {len(input_file_paths)}')
        print(f'Elapsed Time: {elapsed_time}')
        print()
        input('Press ENTER to exit...')

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    finally:
        # Clean up Batch resources
        print('Deleting job')
        batch_client.job.delete(config.JOB_ID)
        #print('Deleting pool')
        #batch_client.pool.delete(config.POOL_ID)
