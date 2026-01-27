# VNStock Automated Analytics & Big Data Pipeline 📈

An end-to-end Data Engineering project that automates the ingestion, processing, and analysis of Vietnamese stock market data (VN30) using the Modern Data Stack.

## 🏗 System Architecture

The pipeline is designed to handle daily/yearly batch processing with a hybrid cloud-local approach:



1.  **Orchestration**: Managed by **Apache Airflow** (running via **Astro CLI**).
2.  **Data Ingestion**: Fetches stock data from **Vnstock API** for 29 tickers.
3.  **Storage (Data Lake)**: Raw and processed data stored in **Google Cloud Storage (GCS)**.
4.  **Compute (Big Data)**: Triggers distributed **PySpark** jobs on **Google Cloud Dataproc** for heavy transformations.
5.  **Analytics Layer**: Aggregates financial KPIs (Volatility, Total Return) and stores them in **PostgreSQL**.
6.  **Containerization**: Entire environment isolated using **Docker**.

## 🚀 Key Features

- **Automated ETL**: Daily ingestion schedules with Airflow DAGs.
- **Stock Recommendation Engine**: Built-in logic using Pandas/PySpark to calculate investment signals (Strong Buy, Hold, etc.).
- **Data Idempotency**: Implemented `UPSERT` logic in PostgreSQL to prevent duplicate records during re-runs.
- **Cloud Integration**: Uses IAM Service Accounts for secure local-to-cloud connectivity.

## 🛠 Tech Stack

| Category | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow (Astro CLI) |
| **Languages** | Python, PySpark, SQL |
| **Cloud (GCP)** | GCS, Dataproc, IAM |
| **Database** | PostgreSQL |
| **Infrastructure** | Docker |
| **Libraries** | Pandas, Vnstock API, Psycopg2 |

## 📂 Project Structure

```text
├── dags/                   # Airflow DAG definitions
├── include/                # Helper scripts and SQL queries
├── plugins/                # Custom Airflow operators/plugins
├── scripts/                # PySpark transformation scripts
├── docker-compose.yaml     # Container orchestration
└── Dockerfile              # Custom Airflow image with dependencies

```

## ⚙️ Getting Started

### Prerequisites

* Docker & Docker Compose
* Astro CLI
* A GCP Account (Service Account JSON key required)

### Setup

1. Clone the repository:
```bash
git clone [https://github.com/tunbq21/airflow_astro_dev.git](https://github.com/tunbq21/airflow_astro_dev.git)
cd airflow_astro_dev

```


2. Set up environment variables in `.env`:
```bash
GCP_CONN_ID='google_cloud_default'
POSTGRES_CONN_ID='postgres_default'

```


3. Start the pipeline:
```bash
astro dev start

```


4. Access Airflow UI at `http://localhost:8080`.

## 📊 Analytics Output

The pipeline generates a `stock_recommendations` table in PostgreSQL with the following schema:

* `ticker`: Stock symbol (e.g., FPT, VNM)
* `volatility`: Standard deviation of returns
* `total_return`: Percentage growth over period
* `signal`: Recommended action (Buy/Sell/Hold)

---

Developed by **Bui Quang Tuan** - *Data Engineer*

2. **Setup:** Nếu project của bạn có thêm các bước cài đặt đặc thù (như config Service Account trên GCP), hãy bổ sung vào mục **Setup**.
3. **Link trong CV:** Sau khi tạo file README này, cái "GitHub Link" trong CV của bạn sẽ trở nên cực kỳ giá trị vì nó giải thích chi tiết những gì bạn đã làm.

Bạn có muốn mình tối ưu thêm đoạn nào trong nội dung này không?

```
