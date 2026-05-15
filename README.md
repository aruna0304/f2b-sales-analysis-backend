# F2B Sales Analysis — Backend

A **FastAPI** backend server powering the Farm2Bag sales analytics platform. Connects to MongoDB for vendor/sales data and serves pre-processed CSV data to the frontend dashboard.

## Project Structure

```
backend/
├── main.py                    # FastAPI app entry point
├── config.py                  # Environment & app configuration
├── pipeline.py                # Data pipeline (generates CSVs from MongoDB)
├── requirements.txt
├── db/
│   ├── mongo_connection.py    # MongoDB client setup
│   └── fetch_data.py          # Raw data fetching helpers
├── processing/
│   ├── clean_data.py          # Data cleaning logic
│   └── aggregate.py           # Aggregation helpers
└── vendor_analysis/
    └── queries.py             # MongoDB aggregation pipelines for vendor analytics
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/data/demand` | Demand intelligence data (from CSV) |
| GET | `/data/historical` | Historical sales data (from CSV) |
| GET | `/vendors/summary` | Vendor purchase summary (MongoDB) |
| GET | `/vendors/profit` | Vendor profit analysis (MongoDB) |
| GET | `/vendors/trends` | Monthly vendor trends (MongoDB) |
| GET | `/vendors/{vendor_id}/products` | Product breakdown per vendor (MongoDB) |

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
Create a `.env` file in this directory:
```env
MONGO_URI="your_mongodb_connection_string"
DB_NAME="your_database_name"
```

### 3. Generate data files
Run the pipeline to produce the CSV files needed by the API:
```bash
python pipeline.py
```

### 4. Start the server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`.  
Interactive docs: `http://localhost:8000/docs`

## Requirements
- Python 3.9+
- MongoDB instance with Farm2Bag sales data
- Frontend: [f2b-sales-analysis-frontend](https://github.com/aruna0304/f2b-sales-analysis-frontend)
