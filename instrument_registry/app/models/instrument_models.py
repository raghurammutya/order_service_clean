"""
SQLAlchemy Models for Instrument Registry Schema

CRITICAL: ALL models specify __table_args__ = {'schema': 'instrument_registry'}
as per ARCHITECTURE_AND_PRODUCTION_STANDARDS.md
"""

from sqlalchemy import (
    Column, String, Integer, Date, Boolean, 
    DateTime, BigInteger, Text, ForeignKey, ARRAY
)
from sqlalchemy.types import DECIMAL
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Instrument(Base):
    """Core instrument registry with hierarchical keys"""
    __tablename__ = 'instruments'
    __table_args__ = {'schema': 'instrument_registry'}
    
    # Primary hierarchical key
    instrument_key = Column(String(100), primary_key=True)
    
    # Broker-specific tokens (legacy, use broker_tokens table)
    kite_token = Column(BigInteger)
    upstox_token = Column(String(50))
    zerodha_token = Column(BigInteger)
    
    # Core instrument attributes
    symbol = Column(String(50), nullable=False)
    name = Column(String(200))
    exchange = Column(String(20), nullable=False)
    segment = Column(String(20), nullable=False)
    instrument_type = Column(String(10), nullable=False)  # EQ, FUT, CE, PE, etc.
    
    # Option/Future specific fields
    underlying_symbol = Column(String(50))
    underlying_instrument_key = Column(String(100))
    strike = Column(DECIMAL(12, 2))
    expiry = Column(Date)
    
    # Trading metadata
    lot_size = Column(Integer, nullable=False, default=1)
    tick_size = Column(DECIMAL(8, 4), default=0.05)
    multiplier = Column(Integer, default=1)
    
    # Classification metadata
    sector = Column(String(100))
    industry = Column(String(100))
    asset_class = Column(String(50), nullable=False)  # equity, commodity, currency, bond, crypto
    
    # Status flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_tradeable = Column(Boolean, nullable=False, default=True)
    is_index = Column(Boolean, nullable=False, default=False)
    
    # Temporal tracking
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    last_refreshed_at = Column(TIMESTAMP)
    
    # Data lineage
    data_source = Column(String(50), nullable=False, default='kite_api')
    refresh_batch_id = Column(String(100))
    data_version = Column(Integer, nullable=False, default=1)
    is_deleted = Column(Boolean, nullable=False, default=False)
    
    # Relationships
    broker_tokens = relationship("BrokerToken", back_populates="instrument")


class BrokerToken(Base):
    """Broker token mappings supporting multiple brokers per instrument"""
    __tablename__ = 'broker_tokens'
    __table_args__ = {'schema': 'instrument_registry'}
    
    id = Column(BigInteger, primary_key=True)
    instrument_key = Column(String(100), ForeignKey('instrument_registry.instruments.instrument_key'), nullable=False)
    broker_name = Column(String(50), nullable=False)  # kite, upstox, ibkr, binance, etc.
    broker_token = Column(String(100), nullable=False)
    broker_symbol = Column(String(100), nullable=False)  # broker-specific symbol format
    
    # Metadata
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    
    # Relationships
    instrument = relationship("Instrument", back_populates="broker_tokens")


class InstrumentEvent(Base):
    """Event store for instrument lifecycle events"""
    __tablename__ = 'instrument_events'
    __table_args__ = {'schema': 'instrument_registry'}
    
    event_id = Column(BigInteger, primary_key=True)
    event_stream_id = Column(String(100), nullable=False)  # instrument_key or batch_id
    event_type = Column(String(50), nullable=False)  # instrument.created, instrument.updated, etc.
    event_version = Column(Integer, nullable=False)
    
    # Event data
    aggregate_id = Column(String(100), nullable=False)  # instrument_key
    aggregate_version = Column(Integer, nullable=False)
    event_data = Column(JSONB, nullable=False)
    event_metadata = Column(JSONB)
    
    # Temporal
    occurred_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    recorded_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)


class AuditTrail(Base):
    """Comprehensive audit trail for all changes to instrument data"""
    __tablename__ = 'audit_trail'
    __table_args__ = {'schema': 'instrument_registry'}
    
    audit_id = Column(BigInteger, primary_key=True)
    instrument_key = Column(String(100), nullable=False)
    operation = Column(String(20), nullable=False)  # INSERT, UPDATE, DELETE, REFRESH, ACTIVATE, DEACTIVATE
    
    # Change tracking
    old_data = Column(JSONB)
    new_data = Column(JSONB)
    changed_fields = Column(ARRAY(Text))
    
    # Metadata
    change_source = Column(String(50), nullable=False, default='api_refresh')
    changed_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    changed_by = Column(String(100))
    refresh_batch_id = Column(String(100))
    
    # Compliance tracking
    compliance_approved = Column(Boolean)
    approved_by = Column(String(100))
    approved_at = Column(TIMESTAMP)


class OptionChain(Base):
    """Derived option chain metadata computed from actual instrument data"""
    __tablename__ = 'option_chains'
    __table_args__ = {'schema': 'instrument_registry'}
    
    id = Column(BigInteger, primary_key=True)
    underlying_symbol = Column(String(50), nullable=False)
    underlying_instrument_key = Column(String(100), nullable=False)
    expiry_date = Column(Date, nullable=False)
    
    # Strike interval analysis
    strike_interval = Column(DECIMAL(10, 2), nullable=False)
    min_strike = Column(DECIMAL(12, 2), nullable=False)
    max_strike = Column(DECIMAL(12, 2), nullable=False)
    strike_count = Column(Integer, nullable=False)
    
    # Metadata
    last_derived_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    refresh_batch_id = Column(String(100))


class DataQualityCheck(Base):
    """Data quality monitoring and validation results"""
    __tablename__ = 'data_quality_checks'
    __table_args__ = {'schema': 'instrument_registry'}
    
    check_id = Column(BigInteger, primary_key=True)
    check_date = Column(Date, nullable=False, default=datetime.utcnow().date)
    check_type = Column(String(50), nullable=False)  # daily_validation, refresh_validation, compliance_check
    
    # Quality metrics
    total_instruments = Column(Integer, nullable=False)
    active_instruments = Column(Integer, nullable=False)
    inactive_instruments = Column(Integer, nullable=False)
    new_instruments = Column(Integer)
    updated_instruments = Column(Integer)
    deprecated_instruments = Column(Integer)
    
    # Issue tracking
    missing_required_fields = Column(Integer)
    duplicate_symbols = Column(Integer)
    stale_instruments = Column(Integer)
    invalid_option_chains = Column(Integer)
    
    # Overall score
    data_quality_score = Column(DECIMAL(5, 2), nullable=False)
    issues_detail = Column(JSONB)
    
    # Performance metrics
    check_duration_ms = Column(Integer)
    refresh_batch_id = Column(String(100))
    
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)