from sqlalchemy import Column, String, DateTime, Integer, Text, Boolean, JSON
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime


class Base(DeclarativeBase):
    pass


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    source_type = Column(String, nullable=False)
    sink_type = Column(String, nullable=False)
    status = Column(String, default="idle")
    last_run_at = Column(DateTime, nullable=True)
    last_watermark = Column(DateTime, nullable=True)
    is_running = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(String, primary_key=True)
    pipeline_id = Column(String, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, default="running")
    records_processed = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)


class SourceCredential(Base):
    __tablename__ = "source_credentials"

    id = Column(String, primary_key=True)
    pipeline_id = Column(String, nullable=False)
    credential_type = Column(String, nullable=False)
    # BUG DF-13: credentials stored as plaintext JSON — no encryption
    credentials = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
