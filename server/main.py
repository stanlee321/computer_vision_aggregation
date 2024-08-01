import os
import json
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from typing import Optional

# File path for storing the DataFrame as a CSV
csv_file_path = "data.csv"

# Function to load or initialize the DataFrame
def load_data():
    if os.path.exists(csv_file_path):
        return pd.read_csv(csv_file_path)
    else:
        # Initialize DataFrame if no CSV exists
        data = {
            "id": [1, 2, 3],
            "remote_path": ["some path"] * 3,
            "original_video": ["some video url"] * 3,
            "video_id": ["some id"] * 3,
            "status": ["some status"] * 3,
            "fps": [2.7] * 3
        }
        df = pd.DataFrame(data)
        df.to_csv(csv_file_path, index=False)  # Save initial data to CSV
        return df
    


# Create a FastAPI instance
app = FastAPI()

# Load or initialize the DataFrame
df = load_data()

def create_id(df):
    return df['id'].max() + 1

class ItemRequest(BaseModel):
    id: Optional[int] = 0
    remote_path: str
    original_video: str
    video_id: str
    status: str
    fps: float
    
class UpdateStatusRequest(BaseModel):
    status: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the Video Handler API!!!"}

def clean_df_for_json(df):
    df = load_data()

    return df.replace([np.inf, -np.inf, np.nan], None).to_dict(orient='records')

@app.get("/items/")
def read_items():
    df = load_data()
    try:
        clean_data = clean_df_for_json(df)
        return clean_data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/items/video_id/{video_id}/")
def read_by_video_id(video_id: str, status: str = Query(
                                default=None, 
                                description="Filter items by status")):
    df = load_data()
    
    # Filter dataframe based on provided video_id and optional status
    if status:
        items = df[(df['video_id'] == video_id) & (df['status'] == status)]
    else:
        items = df[df['video_id'] == video_id]

    if items.empty:
        raise HTTPException(status_code=404, detail="Items not found")
    
    try:
        clean_items = clean_df_for_json(items)
        return clean_items
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
    
@app.get("/items/{item_id}")
def read_item(item_id: int):
    df = load_data()

    item = df[df['id'] == item_id]
    if item.empty:
        raise HTTPException(status_code=404, detail="Item not found")
    try:
        clean_item = clean_df_for_json(item)
        return clean_item[0] if clean_item else {"message": "Item not found"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    

@app.post("/items/")
def create_item(item: ItemRequest):
    df = load_data()
    item.id = create_id(df)
    if item.id in df['id'].values or (item.video_id in df['video_id'].values and item.remote_path in df['remote_path'].values):
        raise HTTPException(status_code=400, detail="Item with this ID already exists")
    df.loc[len(df)] = item.model_dump()
    df.to_csv(csv_file_path, index=False)  # Save updated data to CSV
    return {"message": "Item created successfully", "id": int(item.id)}

@app.put("/items/{item_id}")
def update_item_status(item_id: int, item: UpdateStatusRequest):
    df = load_data()

    # 'status' update 
    new_status = item.status
    if item_id not in df['id'].values:
        raise HTTPException(status_code=404, detail="Item not found")
    df.loc[df['id'] == item_id, ['status']] = new_status
    df.to_csv(csv_file_path, index=False)  # Save updated data to CSV

    return item

@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    df = load_data()

    if item_id not in df['id'].values:
        raise HTTPException(status_code=404, detail="Item not found")
    df = df[df['id'] != item_id]
    df.to_csv(csv_file_path, index=False)  # Save updated data to CSV

    return {"message": "Item deleted successfully"}