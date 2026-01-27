# VNStock Automated Analytics & Big Data Pipeline 📈

An end-to-end Data Engineering project that automates the ingestion, processing, and analysis of Vietnamese stock market data (VN30) using the Modern Data Stack.

## 🏗 System Architecture

The pipeline is designed to handle daily/yearly batch processing with a hybrid cloud-local approach:



1.  **Orchestration**: Managed by **Apache Airflow** (running via **Astro CLI**).
2.  **Data Ingestion**: Fetches stock data from **Vnstock API** for 29 tickers.
3.  **Storage (Data Lake)**: Raw and processed data stored in **Google Cloud Storage (GCS)**.
4.  **Compute (Big Data)**: Triggers distributed **PySpark** jobs on **Google Cloud Dataproc**.
5.  **Analytics Layer**: Aggregates financial KPIs and stores them in **PostgreSQL**.

## ☁️ GCP Infrastructure & Spark execution

To enable large-scale data processing, the pipeline integrates local Airflow with Google Cloud services:

* **GCS as a Landing Zone**: Used for storing raw JSON data (Bronze) and Spark job scripts (`.py`).
* **Dynamic Dataproc Clusters**: The Airflow DAG utilizes `DataprocCreateClusterOperator` to spin up managed Spark clusters on-demand, optimizing costs.
* **Remote Job Submission**: Spark jobs are submitted via `DataprocSubmitJobOperator` with custom initialization actions to install dependencies like `vnstock`.
* **IAM Security**: Scoped with `Dataproc Worker` and `Storage Object Admin` roles for secure connectivity.

## 📊 Data Output Preview

The pipeline doesn't just move data; it generates actionable insights. Below is a snapshot of the processed stock analytics stored in PostgreSQL, showing calculated KPIs and investment signals:

![Stock Analytics Output](https://github.com/tunbq21/airflow_astro_dev/blob/main/image_4c40fb.png?raw=true)

> **Note**: The image above displays automated signals (BUY/SELL/HOLD) based on multi-factor financial analysis including Volatility and Total Return.

## 🛠 Tech Stack

| Category | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow (Astro CLI) |
| **Languages** | Python, PySpark, SQL |
| **Cloud (GCP)** | GCS, Dataproc, IAM |
| **Database** | PostgreSQL |
| **Infrastructure** | Docker |

## ⚙️ Setup Instructions

1.  **GCS Buckets**: Create buckets for landing data and hosting Spark scripts.
2.  **Airflow Connection**: Create a connection named `google_cloud_default` in Airflow UI.
3.  **Service Account**: Upload your GCP Service Account JSON key with Dataproc and Storage permissions.
4.  **Start Pipeline**:
    ```bash
    astro dev start
    ```

---
Developed by **Bui Quang Tuan** - *Data Engineer*
