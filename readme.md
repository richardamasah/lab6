

## 🚀 Project Overview

This project simulates a real-time data pipeline for an e-commerce company to compute order-level and category-level KPIs daily. It uses **AWS ECS**, **S3**, **Lambda**, **Step Functions**, and **DynamoDB**, orchestrated via **event-driven architecture** and **CI/CD with GitHub Actions**. The goal is to ingest raw CSV files from S3, validate them, transform them into business insights, and store the final KPIs in DynamoDB.

---

## 📊 Business Logic / Problem Statement

* Every day, a set of 3 files (`orders`, `order_items`, `products`) is dropped into an S3 bucket under the `raw/` prefix.
* The system must ensure that **all 3 files** are present before triggering a processing pipeline.
* Files must be **validated** for schema and referential integrity.
* After validation, files are **transformed** to compute:

  * Daily revenue, return rate, and average order value per category.
  * Total orders, revenue, items sold, unique customers, and return rate per day.
* Results are saved into **two DynamoDB tables**: `order_kpis` and `category_kpis`.

---

## 🏗️ Architecture + Flow (Step-by-Step Explanation)

```
   [S3 raw/]  -->  [Lambda (File Watcher)]
                      │
                      ├──> Validate headers/schema
                      ├──> Update DynamoDB ingestion registry
                      ├──> Move to validated/ if clean
                      ├──> Rejected folder + reason.json if invalid
                      └──> Trigger Step Function if 3 valid files are detected

   [Step Function] --> [ECS Validator Task] --> [ECS Transformer Task]
                                      │                  │
                                      ▼                  ▼
                            Refer. integrity         Compute KPIs
                            (products, orders)       Store in DynamoDB
                                                      (order_kpis & category_kpis)

                          --> [Lambda Archive Function] --> Move files to archived/
```

### Key Services Used

| Service            | Role                                                                   |
| ------------------ | ---------------------------------------------------------------------- |
| **S3**             | Stores incoming files (`raw/`, `validated/`, `rejected/`, `archived/`) |
| **Lambda**         | File validation, ingestion tracking, Step Function trigger, archiving  |
| **DynamoDB**       | Stores ingestion registry and final KPIs                               |
| **Step Functions** | Orchestrates ECS jobs + retries + error handling                       |
| **ECS Fargate**    | Runs containerized `validator` and `transformer` logic                 |
| **GitHub Actions** | Automates CI/CD to deploy Docker images to ECR and update ECS          |

---

## 📂 Project Scope and Folder Structure

```
ecs-kpi-pipeline/
├── lambda/
│   ├── file_watcher.py
│   └── archive_lambda.py
├── ecs/
│   ├── validator.py
│   └── transformer.py
├── step_function/
│   └── pipeline1.json
├── .github/workflows/
│   └── deploy.yml
├── README_part1.md
├── README_part2.md
└── .env.template
```

---

## 🗃️ DynamoDB Table Schemas

### 🟢 `ingestion_registry`

Tracks which files have arrived and when to trigger Step Functions.

```json
{
  "group_key": "part1",         // From filename suffix
  "orders_path": "validated/orders/orders_part1.csv",
  "order_items_path": "validated/order_items/order_items_part1.csv",
  "products_path": "validated/products/products.csv",
  "has_orders": true,
  "has_order_items": true,
  "order_date": "2025-07-07",
  "debounce_ttl": 1720000000     // UNIX timestamp for debounce expiration
}
```

### 🟢 `order_kpis`

```json
{
  "order_date": "2025-07-07",
  "total_orders": 43,
  "total_revenue": 13500.75,
  "total_items_sold": 221,
  "unique_customers": 38,
  "return_rate": 0.13
}
```

### 🟢 `category_kpis`

```json
{
  "order_date": "2025-07-07",
  "category": "electronics",
  "daily_revenue": 4800.25,
  "avg_order_value": 133.3,
  "avg_return_rate": 0.04
}
```

---

## ⚙️ Project Setup & Deployment

### 1. 🛠️ Clone the Repository

```bash
git clone https://github.com/yourusername/ecs-kpi-pipeline.git
cd ecs-kpi-pipeline
cp .env.template .env   # Fill in S3 bucket, ARNs, etc
```

### 2. 📦 Build Docker Images

```bash
docker build -t validator:latest -f ecs/validator.Dockerfile ./ecs
docker build -t transformer:latest -f ecs/transformer.Dockerfile ./ecs
```

### 3. 🚀 Push to ECR via GitHub Actions

* GitHub Secrets Required: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `ECR_REPO`
* Workflow: `.github/workflows/deploy.yml` will handle ECR push + ECS update

### 4. 🧠 Upload Input Files

Place raw CSVs into `s3://<bucket>/raw/`:

* `orders_part1.csv`
* `order_items_part1.csv`
* `products.csv`

Step Function will start automatically once all 3 validated versions exist.

---

## 🔐 IAM Role Permissions (Minimum Required)

### Lambda Execution Role:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:*",
    "dynamodb:*",
    "states:StartExecution"
  ],
  "Resource": "*"
}
```

### ECS Task Role:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "dynamodb:PutItem"],
  "Resource": "*"
}
```

---
