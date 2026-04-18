import time, csv, os
from azure.synapse.spark import SparkClient
from azure.synapse.spark.models import SparkBatchJobOptions
from azure.identity import DefaultAzureCredential

# Azure connection details
SYNAPSE_ENDPOINT = "https://healthcare-pipeline-synapse.dev.azuresynapse.net"
SPARK_POOL       = "sparkpool"
ACCOUNT          = "healthcarepipelinesvnit"
CONTAINER        = "pipeline"
ADLS_BASE        = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net"
KEY              = os.environ["AZURE_STORAGE_KEY"]

# The experiment: run Gold CPU job with different node counts
# Keep partition count fixed at 500, vary number of Synapse nodes
EXECUTOR_COUNTS = [3, 5, 8, 10]
results = []

credential = DefaultAzureCredential()
client     = SparkClient(credential, SYNAPSE_ENDPOINT, SPARK_POOL)

for n_exec in EXECUTOR_COUNTS:
    out_path = f"{ADLS_BASE}/benchmark_outputs/gold_exec{n_exec}/"

    print(f"\n--- Submitting: executors={n_exec} ---")
    t0 = time.time()

    options = SparkBatchJobOptions(
        name                  = f"gold-exec-{n_exec}-{int(time.time())}",
        file                  = f"{ADLS_BASE}/jobs/gold_cpu_vitals.py",
        args                  = [
            f"--input={ADLS_BASE}/silver/vitals/",
            f"--output={out_path}"
        ],
        executor_count        = n_exec,
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

    # Poll every 30 seconds until the job finishes
    while True:
        status  = client.spark_batch.get_spark_batch_job(job.id)
        elapsed = int(time.time() - t0)
        print(f"  [{elapsed}s] State: {status.state}")
        if status.state in ["success", "dead", "killed", "error"]:
            break
        time.sleep(30)

    total_time = time.time() - t0
    success    = status.state == "success"
    print(f"  DONE. Executors={n_exec} Time={total_time:.0f}s Success={success}")

    results.append({
        'executors':    n_exec,
        'time_seconds': round(total_time),
        'success':      success
    })

# Save results to CSV
out_csv = r"D:\projects\healthcare-pipeline\benchmarks\results\executor_sweep.csv"
with open(out_csv, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=['executors', 'time_seconds', 'success'])
    w.writeheader()
    w.writerows(results)

print(f"\nResults saved to {out_csv}")
for r in results:
    print(f"  Executors={r['executors']}  Time={r['time_seconds']}s  Success={r['success']}")
