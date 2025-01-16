import logging
import azure.functions as func
import logging
import io
import psycopg2.extras
# from get_setting_files import *
from datetime import datetime, timedelta, timezone

from azure import batch as batch
from azure.batch import BatchServiceClient, models as batchmodels, batch_auth as batch_auth
from azure.storage.blob import (
    generate_container_sas,
    ContainerSasPermissions,
)
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()

from typing import Union

user_management_db_url =os.getenv("user_management_db_url")
CONTINUOUS_LEARNING_IMAGE_NAME=os.getenv("CONTINUOUS_LEARNING_IMAGE_NAME")
IMAGE_VERSION=os.getenv("IMAGE_VERSION")

ACR_LOGINSERVER = os.getenv("ACR_LOGINSERVER")
ACR_USERNAME = os.getenv("ACR_USERNAME")
ACR_PASSWORD = os.getenv("ACR_PASSWORD")


AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_PRIMARY_ACCESS_KEY=os.getenv("AZURE_STORAGE_PRIMARY_ACCESS_KEY")
AZURE_STORAGE_ACCOUNT=os.getenv("AZURE_STORAGE_ACCOUNT")
CONTINUOUS_LEARNING_LOGS_CONTAINER_NAME = os.getenv("CONTINUOUS_LEARNING_LOGS_CONTAINER_NAME")


BATCH_ACCOUNT_URL = os.getenv("BATCH_ACCOUNT_URL")
BATCH_ACCOUNT_NAME = os.getenv("BATCH_ACCOUNT_NAME")
BATCH_ACCOUNT_PRIMARY_ACCESS_KEY = os.getenv("BATCH_ACCOUNT_PRIMARY_ACCESS_KEY")

CONTINUOUS_LEARNING_POOL_ID= os.getenv("CONTINUOUS_LEARNING_POOL_ID")
POOL_NODE_COUNT=os.getenv("POOL_NODE_COUNT")
POOL_VM_SIZE=os.getenv("POOL_VM_SIZE")

env_variables = dotenv_values("invoice_index_upload.env")

def invoice_classifier_continuous_learning() -> None:

    image_name = CONTINUOUS_LEARNING_IMAGE_NAME
    image_version = IMAGE_VERSION

    credentials = batch_auth.SharedKeyCredentials(
        BATCH_ACCOUNT_NAME,
        BATCH_ACCOUNT_PRIMARY_ACCESS_KEY,
    )

    batch_client = BatchServiceClient(credentials, batch_url=BATCH_ACCOUNT_URL)

    job_id = f"invoice-classifier-job-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    customer_ids = get_customers()
    try:
        if len(customer_ids):
            container_registry = batch.models.ContainerRegistry(
                registry_server=ACR_LOGINSERVER,
                user_name=ACR_USERNAME,
                password=ACR_PASSWORD,
            )

            create_pool(
                batch_service_client=batch_client,
                pool_id=CONTINUOUS_LEARNING_POOL_ID,
                container_registry=container_registry,
                image_name=image_name + ":" + image_version,
                pool_node_count=POOL_NODE_COUNT,
                pool_vm_size=POOL_VM_SIZE,
            )

            create_job(batch_client, job_id, CONTINUOUS_LEARNING_POOL_ID)
            
            command = "python main.py" 
          
        
            for customer_id  in customer_ids:
                
                task_env_variables = env_variables.copy()  # Copy to avoid modifying original
                task_env_variables["CUSTOMER_ID"] = str(customer_id)
              
                add_task(
                    batch_service_client=batch_client,
                    image_name=image_name,
                    image_version=image_version,
                    job_id=job_id,
                    command=command,
                    name=f"{customer_id}_invoice_classifier_continuous_learning",
                    logs_path_prefix=f'{customer_id}_invoice_classifier_continuous_learning',
                    environment_variables=task_env_variables
                )
                logging.info(f'invoice classifier continuous learning started for - {customer_id}') 
           

    except batchmodels.BatchErrorException as err:
        print(err)
        logging.error(err)

def generate_sas_url_for_logs_container() -> str:

    sas_container = generate_container_sas(
        account_name=AZURE_STORAGE_ACCOUNT_NAME,
        container_name=CONTINUOUS_LEARNING_LOGS_CONTAINER_NAME,
        account_key=AZURE_STORAGE_PRIMARY_ACCESS_KEY,
        permission=ContainerSasPermissions(read=True, write=True, delete=True, list=True, create=True),
        expiry=datetime.utcnow() + timedelta(days=1),
    )
    sas_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTINUOUS_LEARNING_LOGS_CONTAINER_NAME}?{sas_container}"
    return sas_url


def create_pool(
    batch_service_client: BatchServiceClient,
    image_name: str,
    pool_id: str,
    container_registry: batch.models.ContainerRegistry,
    pool_vm_size: str,
    pool_node_count: int,
):
    """
    Creates a new pool in the Azure Batch account.

    Args:
        batch_service_client: The batch client to use for creating the pool.
        image_name: The name of the container image to use for the pool.
        pool_id: The unique ID of the pool to create.
        container_registry: The container registry to use for the pool.
        pool_vm_size: The size of the virtual machines in the pool.
        pool_node_count: The number of nodes to create in the pool.
    """

    logging.info(f"Creating pool [{pool_id}]...")

    # Use the latest version of the Ubuntu server container image.
    image_ref_to_use = batchmodels.ImageReference(
        publisher="microsoft-azure-batch",
        offer="ubuntu-server-container",
        sku="20-04-lts",
        version="latest",
    )
    # Set the container configuration for the pool to use the specified container image.
    container_conf = batch.models.ContainerConfiguration(
        container_image_names=[image_name], container_registries=[container_registry]
    )

    # Define the parameters for the new pool.
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batch.models.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            container_configuration=container_conf,
            node_agent_sku_id="batch.node.ubuntu 20.04",
        ),
        vm_size=pool_vm_size,
        target_low_priority_nodes=pool_node_count,
    )
    # If the pool does not already exist, create it.
    if not batch_service_client.pool.exists(pool_id):
        batch_service_client.pool.add(new_pool)



def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    """
    Creates a new job in the Azure Batch

    Args:
        batch_service_client: The batch client to use for creating the job.
        job_id: The unique ID of the job to create.
        pool_id: The ID of the pool to use for the job.
    """
    logging.info(f"Creating job [{job_id}]...")
    # Define the parameters for the new job.
    job = batch.models.JobAddParameter(
        id=job_id, pool_info=batch.models.PoolInformation(pool_id=pool_id)
    )

    batch_service_client.job.add(job)


def add_task(
    batch_service_client: BatchServiceClient,
    image_name: str,
    image_version: str,
    job_id: str,
    command: str,
    name: str,
    logs_path_prefix: str,
    environment_variables: dict = None,
):
    """
    Args:
        batch_service_client: The Batch client to use.
        image_name (str): The name of the Docker image to use for the task.
        image_version (str): The version of the Docker image to use for the task.
        job_id (str): The ID of the job to which to add the task.
        command (str): The command to run inside the container.
        name (str): The name of the task.
        logs_path_prefix (str): Path in the Azure Batch Storage for logs
        environment_variables (dict): Environment variables to pass to container
    Returns:
    None
    """
    if environment_variables is not None:
        environment_variables = [batchmodels.EnvironmentSetting(name=key, value=value) for key, value in environment_variables.items()]
    logs_container_sas_url = generate_sas_url_for_logs_container()
    user = batchmodels.UserIdentity(
        auto_user=batchmodels.AutoUserSpecification(
            elevation_level=batchmodels.ElevationLevel.admin,
            scope=batchmodels.AutoUserScope.task,
        )
    )

    task_id = name
    task_container_settings = batch.models.TaskContainerSettings(
        image_name=image_name + ":" + image_version,
        container_run_options=f"--rm",
        working_directory="containerImageDefault",
        registry=batch.models.ContainerRegistry(
            registry_server=ACR_LOGINSERVER,
            user_name=ACR_USERNAME,
            password=ACR_PASSWORD,
        ),
    )
    task = batch.models.TaskAddParameter(
        id=task_id,
        command_line=command,
        container_settings=task_container_settings,
        user_identity=user,
        environment_settings=environment_variables,
        output_files=[
            batch.models.OutputFile(
                file_pattern="../std*",
                destination=batch.models.OutputFileDestination(
                    container=batch.models.OutputFileBlobContainerDestination(
                        container_url=logs_container_sas_url,
                        path=f"{logs_path_prefix}/{job_id}",
                    )
                ),
                upload_options=batch.models.OutputFileUploadOptions(
                    upload_condition=batch.models.OutputFileUploadCondition.task_completion
                ),
            ),
        ],
    )
    logging.info("running " + command)

    batch_service_client.task.add(job_id, task)



def destroy_pool_jobs_tasks () -> None:
    

    credentials = batch_auth.SharedKeyCredentials(
        BATCH_ACCOUNT_NAME,
        BATCH_ACCOUNT_PRIMARY_ACCESS_KEY,
    )

    batch_service_client = BatchServiceClient(credentials, batch_url=BATCH_ACCOUNT_URL)

    logging.info(f"Monitoring all tasks for 'Completed' ")
    pools_ids = [
        i.id
        for i in batch_service_client.pool.list()
        if i.id.startswith(('feedback-robot-pool-main'))
    ]
    for pool_id in pools_ids:
        check_pool_for_completed_jobs(
            batch_service_client=batch_service_client, pool_id=pool_id
        )


def check_pool_for_completed_jobs(
    batch_service_client: BatchServiceClient, pool_id: str
):
    # Get all jobs in the pool.
    jobs = [job for job in batch_service_client.job.list() if job.pool_info.pool_id == pool_id]

    # For each job, check if all tasks are completed.
    for job in jobs:
        tasks = batch_service_client.task.list(job.id)

        incomplete_tasks = [
            task for task in tasks if task.state != batchmodels.TaskState.completed
        ]
        if not incomplete_tasks:
            logging.info(f"There are not incomplete tasks for job {job.id}")
            batch_service_client.job.delete(job.id)
    # wait until the last completed that is removed
    # time.sleep(20)
    # check if there are any pending jobs
    jobs = [
        job
        for job in batch_service_client.job.list() if job.pool_info.pool_id == pool_id
        and job.state != batchmodels.JobState.deleting
    ]
    # if len(jobs) == 0:
       
    batch_service_client.pool.delete(pool_id)

# print(get_customers())
def get_customers():
    customer_ids = []
    customer_id=os.getenv('CUSTOMER_ID')
    conn = psycopg2.connect(user_management_db_url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cpa_info={}
    if customer_id=='test':
        print('test feedback files')
        query = """SELECT DISTINCT d.customer_id
                from ditta d 
                join customer c on c._customer_id = d.customer_id 
                where d.is_active = true
                        and c.is_customer_active
                        and d.customer_id = 10010
                        and c.accounting_system_id = 1
                        and (d.is_reconciliation_activated = true);"""
    else:
        query = """
                SELECT DISTINCT d.customer_id
                from ditta d 
                join customer c on c._customer_id = d.customer_id 
                where d.is_active = true
                and c.is_customer_active
                and c.accounting_system_id = 1
                and (d.is_reconciliation_activated = true);
                """
    try:
        cur.execute(query)
        rows = cur.fetchall()  
        customer_ids = [row['customer_id'] for row in rows]

    except Exception as e:
        print(e)
        print("failed to fetch file id")
    finally: 
        cur.close()
        conn.close()   
        return customer_ids

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup= True,
              use_monitor=False) 
def invoiceclassifiercontinuouslearningbatch(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')



        
    logging.info('Python timer trigger function executed.')
    invoice_classifier_continuous_learning()

@app.route(route="invoiceclassifier", methods=["POST"])
def invoice_classifier_http(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # req_body = req.get_json()
        invoice_classifier_continuous_learning()
        return func.HttpResponse(f"Processing for customers started.", status_code=200)
    except ValueError:
        return func.HttpResponse("Invalid input", status_code=400)