from azure.synapse.spark import SparkClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()

client = SparkClient(
    credential=credential,
    endpoint="https://healthcare-synapse-ws.dev.azuresynapse.net",
    spark_pool_name="sparkpool"
)

batch_request = {
    "file": "abfss://pipeline@healthcarepipeline2026.dfs.core.windows.net/scripts/main.py",
    "name": "healthcare-job",
    "executorCount": 2,
    "driverMemory": "4g",
    "executorMemory": "4g"
}

batch = client.spark_batch.create(batch_request)
print(f"Job submitted: {batch.id}")
