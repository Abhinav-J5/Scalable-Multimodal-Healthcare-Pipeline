import pandas as pd
import matplotlib.pyplot as plt
import os

RESULTS = r"D:\projects\healthcare-pipeline\benchmarks\results"
PLOTS   = r"D:\projects\healthcare-pipeline\benchmarks\plots"
os.makedirs(PLOTS, exist_ok=True)

# ── Plot 1: Executor scaling ─────────────────────────────────────────
exec_df = pd.read_csv(f"{RESULTS}\\executor_sweep.csv")
exec_df = exec_df[exec_df.success == True]

baseline = exec_df[exec_df.executors == exec_df.executors.min()]['time_seconds'].values[0]
exec_df['speedup'] = baseline / exec_df.time_seconds

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

ax1.plot(exec_df.executors, exec_df.time_seconds, 'o-', color='steelblue', linewidth=2)
ax1.set_xlabel("Number of executors")
ax1.set_ylabel("Wall time (seconds)")
ax1.set_title("Executor scaling — wall time")
ax1.grid(True, alpha=0.3)

ax2.plot(exec_df.executors, exec_df.speedup, 'o-',
         color='steelblue', linewidth=2, label='Actual speedup')
ax2.plot(exec_df.executors, exec_df.executors / exec_df.executors.min(),
         '--', color='gray', linewidth=1.5, label='Linear ideal')
ax2.set_xlabel("Number of executors")
ax2.set_ylabel("Speedup (x)")
ax2.set_title("Executor scaling — speedup vs ideal")
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(f"{PLOTS}\\executor_scaling.png", dpi=150, bbox_inches='tight')
print("Saved executor_scaling.png")

# ── Plot 2: Partition tuning ─────────────────────────────────────────
part_df = pd.read_csv(f"{RESULTS}\\partition_sweep.csv")
part_df = part_df[part_df.success == True]

plt.figure(figsize=(7, 4))
plt.plot(part_df.partitions, part_df.time_seconds, 's-',
         color='darkorange', linewidth=2)
plt.xlabel("Partition count")
plt.ylabel("Wall time (seconds)")
plt.title("Partition tuning — Silver job wall time")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(f"{PLOTS}\\partition_tuning.png", dpi=150, bbox_inches='tight')
print("Saved partition_tuning.png")

plt.show()
print("All plots saved.")
