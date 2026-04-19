import time, csv, os
from azure.synapse.spark import SparkClient
from azure.synapse.spark.models import SparkBatchJobOptions
from azure.identity import DefaultAzureCredential

SYNAPSE_ENDPOINT = "https://healthcare-pipeline-synapse.dev.azuresynapse.net"
SPARK_POOL       = "sparkpool"
ACCOUNT          = "healthcarepipelinesvnit"
CONTAINER        = "pipeline"
ADLS_BASE        = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net"
KEY              = os.environ["AZURE_STORAGE_KEY"]

# The experiment: run Silver job with different partition counts
# Keep nodes fixed at 5, vary shuffle partitions
PARTITION_COUNTS = [100, 250, 500, 1000]
results = []

credential = DefaultAzureCredential()
client     = SparkClient(credential, SYNAPSE_ENDPOINT, SPARK_POOL)

for n_part in PARTITION_COUNTS:
    out_path = f"{ADLS_BASE}/benchmark_outputs/silver_part{n_part}/"

    print(f"\n--- Submitting: partitions={n_part} ---")
    t0 = time.time()

    options = SparkBatchJobOptions(
        name                  = f"silver-part-{n_part}-{int(time.time())}",
        file                  = f"{ADLS_BASE}/jobs/silver_vitals.py",
        args                  = [
            f"--input={ADLS_BASE}/bronze/vitals/",
            f"--output={out_path}",
            f"--partitions={n_part}"
        ],
        executor_count        = 5,
        executor_core_count   = 4,
        executor_memory_in_mb = 28672,
        driver_core_count     = 4,
        driver_memory_in_mb   = 28672,
        conf = {
            f"spark.hadoop.fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net": KEY,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

    job = client.spark_batch.create_spark_batch_job(options)
    print(f"  Job submitted: {job.id}")

    while True:
        status  = client.spark_batch.get_spark_batch_job(job.id)
        elapsed = int(time.time() - t0)
        print(f"  [{elapsed}s] State: {status.state}")
        if status.state in ["success", "dead", "killed", "error"]:
            break
        time.sleep(30)

    total_time = time.time() - t0
    success    = status.state == "success"
    print(f"  DONE. Partitions={n_part} Time={total_time:.0f}s Success={success}")

    results.append({
        'partitions':   n_part,
        'time_seconds': round(total_time),
        'success':      success
    })

out_csv = r"D:\projects\healthcare-pipeline\benchmarks\results\partition_sweep.csv"
with open(out_csv, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=['partitions', 'time_seconds', 'success'])
    w.writeheader()
    w.writerows(results)

print(f"\nResults saved to {out_csv}")
