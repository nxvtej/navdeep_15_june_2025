"""
Inside modesl handling duplicate entries and when batch process run, it will remain unaffected.
"""
from sqlalchemy import Column, Integer, SmallInteger, String, Text, DateTime, UniqueConstraint, Boolean, Time
from .db import Base
import uuid


class Store(Base):
    __tablename__ = "stores"

    store_id = Column(String, primary_key=True, index=True, unique=True)

# csv headers:- store_id, status, timestamp_utc
class Store_Status(Base):
    __tablename__ = "store_status"

    # columns present
    id = Column(Integer, primary_key=True, autoincrement=True) 
    store_id = Column(String, index=True)
    status = Column(Boolean)
    timestamp_utc = Column(DateTime(timezone=True), index=True)

    # handle duplicates
    __table_args__ = (
        UniqueConstraint('store_id','timestamp_utc', name='uq_store_status'),
    )

# csv headers:- store_id, dayOfWeek, start_time_local, end_time_local
class Menu_Hours(Base):
    __tablename__ = "menu_hours"

    # columns present
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String, index=True)
    day_of_week = Column(SmallInteger)
    start_time_local = Column(Time)
    end_time_local = Column(Time)

    # handle duplicates
    __table_args__ = (
        UniqueConstraint('store_id', 'day_of_week', 'start_time_local', 'end_time_local', name='uq_menu_hours'),
    )

# csv headers:- store_id, timezone_str
class Timezone(Base):
    __tablename__ = "timezones"

    # columns present
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String, unique=True)
    timezone_str = Column(String)

    __table_args__ = (
        UniqueConstraint('store_id', 'timezone_str', name='uq_timezone'),
    )

#  metadata for report generation
class Report(Base):
    __tablename__ = "reports"

    report_id = Column(String, primary_key=True, unique=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, default="pending")
    created_by = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    report_file_path = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint('report_id', name='uq_report_id'),
    )