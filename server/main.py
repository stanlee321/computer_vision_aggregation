import sqlite3
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from contextlib import asynccontextmanager, contextmanager
import os

# Database configuration
DATABASE_NAME = os.getenv('DATABASE_NAME', 'video_handler.db')

def init_db():
    """Initialize the SQLite database and create the items table if it doesn't exist"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            remote_path TEXT NOT NULL,
            original_video TEXT NOT NULL,
            video_id TEXT NOT NULL,
            status TEXT NOT NULL,
            kind TEXT NOT NULL,
            fps REAL NOT NULL
        )
    ''')
    
    conn.commit()
    conn.close()

def dict_factory(cursor, row):
    """Convert SQLite row to dictionary"""
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def get_db_connection():
    """Create a database connection that returns rows as dictionaries"""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = dict_factory
    return conn

@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Define lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles database initialization and cleanup.
    """
    # Startup: Initialize the database
    init_db()
    yield
    # Shutdown: Clean up database connections
    try:
        conn = get_db_connection()
        conn.close()
    except Exception:
        pass

# Create a FastAPI instance with lifespan
app = FastAPI(lifespan=lifespan)

class ItemRequest(BaseModel):
    id: Optional[int] = None
    remote_path: str
    original_video: str
    video_id: str
    status: str
    kind: str
    fps: float

class UpdateStatusRequest(BaseModel):
    status: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the Video Handler API!!!"}

@app.get("/items/")
def read_items():
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM items ORDER BY id")
            items = cursor.fetchall()
            if not items:
                return []
            return items
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/items/video_id/{video_id}/")
def read_by_video_id(
    video_id: str,
    status: str = Query(default=None, description="Filter items by status"),
    kind: str = Query(default=None, description="Filter items by kind")
):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = ["SELECT * FROM items WHERE video_id = ?"]
            params = [video_id]
            
            if status is not None:
                query.append("AND status = ?")
                params.append(status)
            if kind is not None:
                query.append("AND kind = ?")
                params.append(kind)
            
            final_query = " ".join(query) + " ORDER BY id"
            cursor.execute(final_query, params)
            items = cursor.fetchall()
            
            if not items:
                raise HTTPException(status_code=404, detail="Items not found")
            
            return items
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/items/{item_id}")
def read_item(item_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM items WHERE id = ?", (item_id,))
        item = cursor.fetchone()
        conn.close()
        
        if not item:
            raise HTTPException(status_code=404, detail="Item not found")
        
        return item
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/items/")
def create_item(item: ItemRequest):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT id FROM items WHERE (video_id = ? AND remote_path = ?) OR id = ?",
                (item.video_id, item.remote_path, item.id if item.id else -1)
            )
            if cursor.fetchone():
                raise HTTPException(
                    status_code=400, 
                    detail="Item with this video_id and remote_path combination or ID already exists"
                )
            
            if not isinstance(item.fps, (int, float)) or item.fps <= 0:
                raise HTTPException(status_code=400, detail="FPS must be a positive number")
            
            if item.kind not in ['ground', 'fine']:
                raise HTTPException(status_code=400, detail="Kind must be either 'ground' or 'fine'")
            
            cursor.execute("""
                INSERT INTO items (remote_path, original_video, video_id, status, kind, fps)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (item.remote_path, item.original_video, item.video_id, item.status, item.kind, item.fps))
            
            new_id = cursor.lastrowid
            
            cursor.execute("SELECT * FROM items WHERE id = ?", (new_id,))
            created_item = cursor.fetchone()
            
            return {"message": "Item created successfully", "id": new_id, "item": created_item}
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/items/{item_id}")
def update_item_status(item_id: int, item: UpdateStatusRequest):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            if not item.status:
                raise HTTPException(status_code=400, detail="Status cannot be empty")
            
            cursor.execute("SELECT id FROM items WHERE id = ?", (item_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Item not found")
            
            cursor.execute(
                "UPDATE items SET status = ? WHERE id = ?",
                (item.status, item_id)
            )
            
            cursor.execute("SELECT * FROM items WHERE id = ?", (item_id,))
            updated_item = cursor.fetchone()
            return updated_item
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM items WHERE id = ?", (item_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Item not found")
        
        cursor.execute("DELETE FROM items WHERE id = ?", (item_id,))
        conn.commit()
        conn.close()
        
        return {"message": "Item deleted successfully"}
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    