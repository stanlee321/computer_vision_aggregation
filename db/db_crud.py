from db.base import CRUDInterface
import sqlite3

from typing import Any, Dict, List


class DatabaseCRUD(CRUDInterface):
    def __init__(self, db_path: str, table_name: str):
        self.conn = sqlite3.connect(db_path)
        self.table_name = table_name

    def create(self, data: Dict[str, Any]) -> None:
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        sql = f'INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})'
        self.conn.execute(sql, tuple(data.values()))
        self.conn.commit()

    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        columns = ' AND '.join([f'{k}=?' for k in query.keys()])
        sql = f'SELECT * FROM {self.table_name} WHERE {columns}'
        cursor = self.conn.execute(sql, tuple(query.values()))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def update(self, query: Dict[str, Any], data: Dict[str, Any]) -> None:
        set_clause = ', '.join([f'{k}=?' for k in data.keys()])
        where_clause = ' AND '.join([f'{k}=?' for k in query.keys()])
        sql = f'UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}'
        self.conn.execute(sql, tuple(data.values()) + tuple(query.values()))
        self.conn.commit()

    def delete(self, query: Dict[str, Any]) -> None:
        where_clause = ' AND '.join([f'{k}=?' for k in query.keys()])
        sql = f'DELETE FROM {self.table_name} WHERE {where_clause}'
        self.conn.execute(sql, tuple(query.values()))
        self.conn.commit()