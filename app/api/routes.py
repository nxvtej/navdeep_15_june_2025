from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session as DBSession
from datetime import datetime, timezone
import uuid
import os

from app.database.db import get_db
from app.database.db import engine, Base

from app.database.models import Report
from business.generate_report import generate_report_data_and_save_csv


router = APIRouter()

@router.get("/")
async def read_root():
    return {"message": "Welcome to the Store Monitoring API. Use /trigger_report to start a report."}
    

@router.post("/trigger_report", status_code=status.HTTP_202_ACCEPTED)
async def trigger_report(background_tasks: BackgroundTasks, db: DBSession = Depends(get_db)):
    """
    Triggers the generation of an uptime/downtime report as a background task.
    Returns a report_id to poll for status.
    """

    report_id = str(uuid.uuid4())
    
    new_report = Report(
        report_id=report_id,
        status="Pending", 
        created_at=datetime.now(timezone.utc)
    )
    db.add(new_report)
    db.commit()
    db.refresh(new_report)
    print(f"Report {report_id} created with status 'Pending'.")
    
    background_tasks.add_task(generate_report_data_and_save_csv, report_id)
    return {"report_id": report_id, "status": "Queued", "message": "Report generation started in background."}

@router.get("/get_report/{report_id}")
async def get_report(report_id: str, db: DBSession = Depends(get_db)):
    """
    Checks the status of a report or returns the generated CSV file if complete.
    """
    report_entry = db.query(Report).filter(Report.report_id == report_id).first()

    if not report_entry:
        raise HTTPException(status_code=404, detail="Report ID not found.")

    if report_entry.status in ["Running", "Pending"]:
        return {"status": report_entry.status, "message": "Report is still being generated. Please try again later."}
    
    if report_entry.status == "Failed":
        raise HTTPException(status_code=500, detail={"status": report_entry.status, "message": f"Report generation failed: {report_entry.error_message}"})

    if report_entry.status == "Completed":
        if not report_entry.report_file_path or not os.path.exists(report_entry.report_file_path):
            raise HTTPException(status_code=500, detail="Report file not found or path invalid. Report status is 'Completed' but file is missing.")
        
        return FileResponse(report_entry.report_file_path, media_type="text/csv", filename=f"report_{report_id}.csv")

    raise HTTPException(status_code=500, detail="Unexpected report status.")