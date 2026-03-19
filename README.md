# Scalable Multimodal Healthcare Pipeline

A cloud-native, distributed pipeline for scalable healthcare feature engineering across multimodal clinical data, including time-series vitals, clinical text, and medical imaging.

---

## Overview

Healthcare AI systems require integrating heterogeneous data sources into machine learning–ready representations. Traditional CPU-based pipelines are slow, and limited local GPU resources restrict scalability.

This project implements a **distributed, cost-efficient pipeline** using Google Cloud to process large-scale healthcare data and generate embeddings using hybrid CPU–GPU execution.

---

## Key Features
 
- Multimodal data integration (text, images, time-series)  
- GPU-accelerated embedding generation  
- Reproducible data storage with Delta Lake (time travel support)  
- Scalable and serverless architecture on GCP  
- Performance and cost benchmarking  

---

## Tech Stack

- **Processing**: PySpark  
- **Storage**: Google Cloud Storage (GCS), Delta Lake  
- **Compute**: Dataproc Serverless, Google Colab (GPU)  
- **Models**: ClinicalBERT (text), ResNet-18 (images)  
- **Orchestration**: Bash scripts  
- **Configuration**: YAML  

---

## Architecture

The pipeline follows a hybrid distributed architecture:

Refer to `architecture/flow-diagram.png`

---

## Project Structure

```text
healthcare-scalable-ai/
│
├── README.md
├── architecture/
│   └── flow-diagram.png
│
├── data_pipeline/
│   ├── 
│
├── notebooks/
│   └──
│
├── configs/
│   └──
│
├── scripts/
│   └──
│
└── requirements.txt