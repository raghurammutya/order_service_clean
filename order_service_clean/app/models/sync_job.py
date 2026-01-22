"""
Sync Job Model

Tracks background synchronization jobs for trades, orders, and positions.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Date, Text, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SyncJob(Base):
    """
    Model for tracking synchronization jobs.

    Tracks historical trade/order syncs with status, statistics, and errors.
    """
    __tablename__ = "sync_jobs"
    # __table_args__ = {'schema': 'order_service'}  # REMOVED: Schema causes SQLAlchemy errors

    id = Column(Integer, primary_key=True, index=True)

    # Job metadata
    job_type = Column(String(50), nullable=False, index=True)  # 'trade_sync', 'order_sync', 'position_sync'
    user_id = Column(Integer, nullable=False, index=True)
    trading_account_id = Column(String(100), nullable=False)

    # Status tracking
    status = Column(String(20), nullable=False, default='pending', index=True)  # pending, running, completed, failed

    # Date range (for historical syncs)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)

    # Execution tracking
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # Statistics
    records_fetched = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)

    # Error details
    error_message = Column(Text, nullable=True)
    error_details = Column(JSON, nullable=True)  # List of error messages

    # Trigger information
    triggered_by = Column(String(50), nullable=False)  # 'manual', 'scheduled', 'api'
    trigger_metadata = Column(JSON, nullable=True)  # Additional context

    # Audit
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return (
            f"<SyncJob(id={self.id}, type='{self.job_type}', "
            f"status='{self.status}', user={self.user_id})>"
        )

    def to_dict(self) -> dict:
        """Convert sync job to dictionary"""
        return {
            'id': self.id,
            'job_type': self.job_type,
            'user_id': self.user_id,
            'trading_account_id': self.trading_account_id,
            'status': self.status,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_seconds': self.duration_seconds,
            'records_fetched': self.records_fetched,
            'records_created': self.records_created,
            'records_updated': self.records_updated,
            'records_skipped': self.records_skipped,
            'errors_count': self.errors_count,
            'error_message': self.error_message,
            'error_details': self.error_details,
            'triggered_by': self.triggered_by,
            'trigger_metadata': self.trigger_metadata,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
