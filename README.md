# 🏦 Financial Data Warehouse with Cassandra & Alpha Vantage

## 📌 Overview
This project implements a financial data warehouse using Apache Cassandra (via DataStax Astra DB), enabling stock market data ingestion from Alpha Vantage, storage, analytics, machine learning price prediction, and real-time dashboard visualization — all through clean REST APIs.

## 🚀 Key Features
- Real-time stock data ingestion from Alpha Vantage API
- Scalable, distributed Cassandra-based time series storage
- RESTful endpoints for data access and aggregation
- Predefined financial aggregations (e.g., average volume)
- Price predictions powered by machine learning
- Interactive dashboard for visual analysis

## ⚙️ Prerequisites
- Python 3.8+
- A [DataStax Astra DB](https://www.datastax.com/astra) account and Secure Connect bundle
- An [Alpha Vantage](https://www.alphavantage.co/) API key
- Internet connection + port 8000 available

---

## 🛠️ Setup Instructions

### 1. Environment Setup
```bash
git clone https://github.com/yourusername/dwarehouse_python.git
cd dwarehouse_python

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration
Create a `.env` file in the root directory:

```ini
# Astra DB Configuration
ASTRA_DB_ID=your-database-id
ASTRA_DB_REGION=your-region
ASTRA_DB_KEYSPACE=your-keyspace
ASTRA_DB_APPLICATION_TOKEN=your-astra-token

# Alpha Vantage
ALPHA_VANTAGE_API_KEY=your-alpha-vantage-key
```

### 3. Database Initialization
```bash
python setup_db.py
```

---

## ▶️ Running the Application
```bash
uvicorn main:app --reload --port 8000
```
---

## 📡 Using the API

### Data Ingestion Example
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/ingest/IBM?start=2024-01-01&end=2024-12-31" -Method Post
```

### Core Endpoints

| Endpoint                                       | Method | Description                      |
|-----------------------------------------------|--------|----------------------------------|
| `/assets/{symbol}`                            | POST   | Create a new asset entry         |
| `/ingest/{symbol}`                            | POST   | Ingest time series data          |
| `/assets/{asset_id}`                          | GET    | Get asset metadata               |
| `/data-sources/{data_source_id}`              | GET    | Get data source details          |
| `/time-series/{asset_id}/{data_source_id}`    | GET    | Query raw time series data       |
| `/aggregations/avg-volume/{asset_id}`         | GET    | Average volume aggregation       |
| `/dashboard/{asset_id}`                       | GET    | Interactive dashboard interface  |

---

## 🧠 Machine Learning

To generate or refresh prediction data:

```bash
python model_training.py
```

> ⚠️ Important: **After ingesting new data**, run `model_training.py` to update the prediction table used by the dashboard.

---

## 📊 Data Aggregation

Generate or refresh precomputed aggregations (e.g. average volume):

```bash
python aggregation.py
```

---

## 📺 Dashboard

The dashboard provides a clean UI for viewing predictions and actual price data. Visit:

```
http://localhost:8000/dashboard/IBM
```

---

## 📁 Project Structure

```
├── controllers.py        # FastAPI endpoint definitions
├── cassandra_service.py  # Cassandra connection and helpers
├── app_services.py       # Business logic and orchestrators
├── repositories.py       # Reusable DB access patterns
├── entities.py           # Data model and DTO definitions
├── model_training.py     # ML: train and write predictions
├── aggregation.py        # Time series data aggregators
├── initialize_data.py    # Insert core assets & sources
├── setup_db.py           # Initialize schema and keyspace
├── database.py           # Astra DB & Secure Connect config
└── requirements.txt      # Dependencies
```

---

## 🧯 Troubleshooting

- **🔐 Secure Connect Bundle:** Place in `data/secure-connect-dw-cassandra.zip`
- **🌐 Port Conflicts:** Ensure port `8000` is available before running
- **🔑 Astra Token Permissions:** Verify that your token allows schema & data writes
- **📉 Alpha Vantage Limits:** Free tier allows 5 requests/min — consider batching
