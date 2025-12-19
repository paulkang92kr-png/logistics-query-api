from typing import Optional, Literal
from fastapi import FastAPI
from pydantic import BaseModel, Field
import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="Logistics Query API", version="0.1")

class QueryRequest(BaseModel):
    start_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    shipper: Optional[str] = None
    region: Optional[str] = None
    imex: Optional[str] = None
    manager: Optional[str] = None
    group_by: Optional[Literal["work_date","shipper","region","manager","imex"]] = None
    top_n: int = 50
    limit: int = 200

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/query")
def query(req: QueryRequest):
    where = []
    params = {}

    if req.start_date:
        where.append("work_date >= :start_date")
        params["start_date"] = req.start_date
    if req.end_date:
        where.append("work_date <= :end_date")
        params["end_date"] = req.end_date
    if req.shipper:
        where.append("shipper = :shipper")
        params["shipper"] = req.shipper
    if req.region:
        where.append("region = :region")
        params["region"] = req.region
    if req.imex:
        where.append("imex = :imex")
        params["imex"] = req.imex
    if req.manager:
        where.append("manager = :manager")
        params["manager"] = req.manager

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    if req.group_by:
        gb = req.group_by
        sql = f"""
        SELECT
          {gb} AS group_key,
          SUM(trip_count)  AS trip_count,
          SUM(sales_sum)   AS sales_sum,
          SUM(payment_sum) AS payment_sum,
          SUM(margin_sum)  AS margin_sum
        FROM daily_summary
        {where_sql}
        GROUP BY {gb}
        ORDER BY sales_sum DESC
        LIMIT :top_n
        """
        params["top_n"] = max(1, min(req.top_n, 200))
    else:
        sql = f"""
        SELECT *
        FROM daily_summary
        {where_sql}
        ORDER BY work_date DESC
        LIMIT :limit
        """
        params["limit"] = max(1, min(req.limit, 1000))

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return {"rows": rows, "count": len(rows)}
