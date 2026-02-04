from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import glob
import os

app = FastAPI()

# Allow CORS (Cross-Origin requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "Crypto Sentinel API is Live"}

@app.get("/latest")
def get_latest_data():
    # 1. Find the most recent parquet file
    files = glob.glob("spark_data/storage/*.parquet")
    if not files:
        return {"error": "No data found"}
    
    # Sort by time to get the newest one
    latest_file = max(files, key=os.path.getmtime)
    
    try:
        # 2. Read it and convert to JSON
        df = pd.read_parquet(latest_file)
        
        # Sort by timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
            
        # Return the last 50 data points
        data = df.tail(50).to_dict(orient="records")
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}