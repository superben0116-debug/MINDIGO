from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app import models

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    prog = db.query(models.ImportJobProgress).filter(models.ImportJobProgress.job_id == job_id).first()
    logs = (
        db.query(models.ImportJobLog)
        .filter(models.ImportJobLog.job_id == job_id)
        .order_by(models.ImportJobLog.id.desc())
        .limit(20)
        .all()
    )
    return {
        "id": job.id,
        "status": job.status,
        "success": job.success_count,
        "failed": job.failed_count,
        "start_time": job.start_time,
        "end_time": job.end_time,
        "error_summary": job.error_summary,
        "progress": {
            "total": prog.total if prog else 0,
            "processed": prog.processed if prog else 0,
            "success": prog.success if prog else 0,
            "failed": prog.failed if prog else 0,
        },
        "logs": [{"level": l.level, "message": l.message, "created_at": l.created_at} for l in logs],
    }
