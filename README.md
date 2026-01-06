# Airflow Data Workflows

## Project Overview
This project demonstrates a complete data engineering workflow using Apache Airflow and Docker. 
It includes five DAGs to showcase ETL, data transformation, conditional workflows, and notifications.

### DAGs Included
1. **csv_to_postgres_ingestion** – Ingest employee CSV data into PostgreSQL.
2. **data_transformation_pipeline** – Transform raw employee data and categorize fields.
3. **postgres_to_parquet_export** – Export transformed data to Parquet format.
4. **conditional_workflow_pipeline** – Branching logic based on the day of the week.
5. **notification_workflow** – Risky operations with success/failure notifications.

## Prerequisites
- Docker & Docker Compose installed
- Python 3.11+ and pip
- Git

## Setup Instructions

1. Clone the repository:
```powershell
git clone https://github.com/Divyasrirowtu/airflow-data-workflows.git
cd airflow-data-workflows

2.Build and start Airflow:

docker-compose up -d


3.Access Airflow UI at: http://localhost:8080

4.Initialize Airflow (if first run):

docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

Running DAGs

Trigger DAGs manually via the Airflow UI or set automatic schedules:

DAG 1: Daily

DAG 2: Daily

DAG 3: Weekly

DAG 4: Daily

DAG 5: Daily

Unit Tests

Run tests to verify DAG structure:

pip install pytest
pytest tests/ -v

Folder Structure
project-root/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── dags/
├── tests/
├── data/
├── output/
└── plugins/

Troubleshooting

If Airflow UI doesn’t load, check logs:

docker-compose logs


If DAGs don’t appear, ensure dags/ volume is mounted correctly.


---

### **5️⃣ Save the README.md**

---

### **6️⃣ Commit and push the README.md**

```powershell
git add README.md
git commit -m "Step 9: Added comprehensive README.md for project setup and DAG instructions"
git push origin main

7️⃣ Final Verification

Make sure all DAGs are visible in the Airflow UI.

Run pytest tests/ -v to confirm unit tests pass.

Check Docker containers are running:

docker-compose ps


Verify Parquet output folder exists:

dir .\output