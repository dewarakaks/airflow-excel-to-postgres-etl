# Airflow Excel to Postgres ETL

An automated ETL pipeline that extracts data from Excel files, transforms it, and loads it into a PostgreSQL database using Apache Airflow with Docker.

## Overview

This project implements a robust ETL workflow that:
- Monitors an incoming folder for Excel files (.xlsx)
- Validates and transforms data with proper type casting and cleaning
- Performs upsert operations (INSERT with ON CONFLICT UPDATE) to PostgreSQL
- Archives successfully processed files and isolates error files
- Provides full Airflow orchestration with dynamic task mapping

## Features

- **Automated File Discovery**: Scans incoming directory for Excel files
- **Data Validation**: Ensures required columns exist and data types are correct
- **Data Transformation**: Cleans, casts, and formats data for database ingestion
- **Upsert Logic**: Prevents duplicates using ON CONFLICT for idempotent loads
- **Error Handling**: Moves problematic files to error directory with detailed logging
- **File Archiving**: Automatically archives processed files
- **Dynamic Task Mapping**: Processes multiple Excel files in parallel
- **Docker-based**: Fully containerized with Docker Compose

## Requirements

### System Requirements
- Docker Desktop (or Docker Engine + Docker Compose)
- At least 4GB RAM allocated to Docker
- 10GB free disk space

### Python Dependencies
See `requirements.txt`:
- `pandas` - Data manipulation and Excel reading
- `openpyxl` - Excel file format support
- `psycopg2-binary` - PostgreSQL adapter
- `apache-airflow` - Workflow orchestration

## Project Structure

```
airflow-excel-to-postgres-etl/
├── dags/
│   └── excel_to_postgres.py    # Main Airflow DAG definition
├── data/
│   ├── incoming/                # Place Excel files here
│   ├── archive/                 # Successfully processed files
│   └── error/                   # Files with processing errors
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore patterns
└── README.md                    # This file
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/dewarakaks/airflow-excel-to-postgres-etl.git
cd airflow-excel-to-postgres-etl
```

### 2. Create Data Directories

```bash
mkdir -p data/incoming data/archive data/error
```

### 3. Set Up Docker Compose

Create a `docker-compose.yml` file in the root directory:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s

  airflow-init:
    image: apache/airflow:2.7.0
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: airflow
      _AIRFLOW_WWW_USER_PASSWORD: airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create -u airflow -p airflow -f Admin -l User -r Admin -e admin@example.com

  airflow-webserver:
    image: apache/airflow:2.7.0
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.7.0
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    command: scheduler

volumes:
  postgres-db-volume:
```

### 4. Install Python Dependencies in Airflow

Create a custom Dockerfile (optional) or install packages in running containers:

```bash
docker exec -it <airflow-webserver-container> pip install -r /opt/airflow/dags/../requirements.txt
docker exec -it <airflow-scheduler-container> pip install -r /opt/airflow/dags/../requirements.txt
```

### 5. Create PostgreSQL Connection in Airflow

1. Open Airflow UI: http://localhost:8080
2. Login with `airflow` / `airflow`
3. Go to **Admin → Connections**
4. Click **+** to add a new connection:
   - **Connection Id**: `pg_dw`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Schema**: `airflow` (or your target database)
   - **Login**: `airflow`
   - **Password**: `airflow`
   - **Port**: `5432`
5. Click **Save**

### 6. Create Target Table in PostgreSQL

Connect to PostgreSQL and create the target table:

```sql
CREATE TABLE IF NOT EXISTS sales_daily (
    id BIGINT PRIMARY KEY,
    salescode TEXT NOT NULL,
    dateorder DATE NOT NULL,
    totalsales NUMERIC(15,2) DEFAULT 0.0,
    brand TEXT
);
```

## Usage

### 1. Prepare Excel Files

Create Excel files with the following required columns:
- `id` - Unique identifier (integer)
- `name_sales_code` - Sales code (text)
- `date_order` - Order date (date format)
- `amount_total` - Total sales amount (numeric)
- `brand` - Brand name (text, optional)

Example:

| id  | name_sales_code | date_order | amount_total | brand    |
|-----|----------------|------------|--------------|----------|
| 1   | SC001          | 2025-01-15 | 15000.50     | BrandA   |
| 2   | SC002          | 2025-01-16 | 22500.00     | BrandB   |

### 2. Place Files in Incoming Directory

```bash
cp your-excel-file.xlsx data/incoming/
```

### 3. Start Docker Services

```bash
docker-compose up -d
```

### 4. Trigger the DAG

1. Open Airflow UI: http://localhost:8080
2. Find the DAG: `excel_to_postgres_daily`
3. Toggle it **ON** (if needed)
4. Click the **Play** button to trigger manually

### 5. Monitor Progress

- View task logs in the Airflow UI
- Check the **Graph View** or **Grid View** for task status
- Successful files will be moved to `data/archive/`
- Failed files will be moved to `data/error/`

## ETL Workflow Details

The DAG consists of three main tasks:

### Task 1: `list_excel_files`
- Scans `data/incoming/` for Excel files
- Filters out temporary files (starting with `~$`)
- Fails if no files are found
- Returns list of file paths

### Task 2: `validate_and_transform` (Dynamic Task Mapping)
- Reads each Excel file using pandas
- Validates required columns exist
- Renames columns to match database schema
- Casts data types (int, string, date, numeric)
- Cleans data (strips whitespace, fills nulls)
- Drops invalid rows
- Saves transformed data to temporary CSV
- Moves problematic files to error directory

### Task 3: `load_upsert` (Dynamic Task Mapping)
- Creates temporary staging table in PostgreSQL
- Uses COPY command for fast bulk loading
- Performs upsert using `ON CONFLICT (id) DO UPDATE`
- Commits transaction
- Deletes temporary CSV
- Archives original Excel file

## Column Mapping

| Excel Column      | Database Column | Type    | Required |
|-------------------|-----------------|---------|----------|
| id                | id              | BIGINT  | Yes      |
| name_sales_code   | salescode       | TEXT    | Yes      |
| date_order        | dateorder       | DATE    | Yes      |
| amount_total      | totalsales      | NUMERIC | Yes      |
| brand             | brand           | TEXT    | No       |

## Troubleshooting

### No files found error
- Ensure files are in `data/incoming/` directory
- Check file permissions
- Verify Docker volume mount is correct

### Column validation errors
- Check Excel file has all required column headers
- Column names are case-sensitive
- Ensure no extra spaces in column names

### Database connection errors
- Verify PostgreSQL connection `pg_dw` exists in Airflow
- Check database credentials
- Ensure target table `sales_daily` exists

### Import errors in Airflow
- Install missing Python packages in both webserver and scheduler containers
- Restart Airflow services after installing packages

## Configuration

Key configurations in `dags/excel_to_postgres.py`:

```python
BASE_DIR = Path("/opt/airflow/data")  # Base directory in container
INCOMING_DIR = BASE_DIR / "incoming"  # Where to place Excel files
ARCHIVE_DIR = BASE_DIR / "archive"   # Successfully processed files
ERROR_DIR = BASE_DIR / "error"       # Failed processing files

# PostgreSQL connection ID (configured in Airflow UI)
postgres_conn_id="pg_dw"

# Target table name
table_name = "sales_daily"
```

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is open source and available under the MIT License.

## Author

Created by [dewarakaks](https://github.com/dewarakaks)

## Acknowledgments

- Apache Airflow for workflow orchestration
- Pandas for data manipulation
- PostgreSQL for robust data storage
