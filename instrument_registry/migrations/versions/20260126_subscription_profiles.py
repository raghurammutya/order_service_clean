"""Add subscription profile management tables

Revision ID: 002_subscription_profiles
Revises: 001_initial_schema
Create Date: 2026-01-26 03:05:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002_subscription_profiles'
down_revision: Union[str, None] = '001_initial_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add subscription profile management tables"""
    
    # Create subscription_profiles table
    op.create_table(
        'subscription_profiles',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('profile_id', sa.String(100), nullable=False),
        sa.Column('user_id', sa.String(50), nullable=False),
        sa.Column('profile_name', sa.String(255), nullable=False),
        sa.Column('subscription_type', sa.String(50), nullable=False),  # 'live_feed', 'historical', 'alerts'
        sa.Column('instruments', postgresql.JSONB(), nullable=False),  # Array of instrument keys
        sa.Column('preferences', postgresql.JSONB(), nullable=True),   # User preferences
        sa.Column('validation_rules', postgresql.JSONB(), nullable=True),  # Custom validation rules
        sa.Column('max_instruments', sa.Integer(), nullable=True),     # Per-profile limit
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('profile_id'),
        sa.Index('idx_subscription_profiles_user_id', 'user_id'),
        sa.Index('idx_subscription_profiles_type', 'subscription_type'),
        sa.Index('idx_subscription_profiles_active', 'is_active'),
        schema='instrument_registry'
    )
    
    # Create subscription_audit_log table
    op.create_table(
        'subscription_audit_log',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('audit_id', sa.String(100), nullable=False),
        sa.Column('profile_id', sa.String(100), nullable=True),
        sa.Column('user_id', sa.String(50), nullable=False),
        sa.Column('action', sa.String(50), nullable=False),  # 'create', 'update', 'delete', 'activate', 'deactivate'
        sa.Column('entity_type', sa.String(50), nullable=False),  # 'subscription_profile', 'subscription_conflict'
        sa.Column('entity_id', sa.String(100), nullable=True),
        sa.Column('old_data', postgresql.JSONB(), nullable=True),
        sa.Column('new_data', postgresql.JSONB(), nullable=True),
        sa.Column('metadata', postgresql.JSONB(), nullable=True),  # Additional audit context
        sa.Column('ip_address', sa.String(45), nullable=True),
        sa.Column('user_agent', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('audit_id'),
        sa.Index('idx_subscription_audit_profile_id', 'profile_id'),
        sa.Index('idx_subscription_audit_user_id', 'user_id'),
        sa.Index('idx_subscription_audit_action', 'action'),
        sa.Index('idx_subscription_audit_created_at', 'created_at'),
        schema='instrument_registry'
    )
    
    # Create subscription_conflicts table
    op.create_table(
        'subscription_conflicts',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('conflict_id', sa.String(100), nullable=False),
        sa.Column('profile_id', sa.String(100), nullable=False),
        sa.Column('user_id', sa.String(50), nullable=False),
        sa.Column('conflict_type', sa.String(50), nullable=False),  # 'limit_exceeded', 'duplicate_subscription', 'invalid_instrument'
        sa.Column('conflict_data', postgresql.JSONB(), nullable=False),
        sa.Column('resolution_strategy', sa.String(50), nullable=True),  # From config: 'latest_wins', 'merge', 'fail_on_conflict'
        sa.Column('status', sa.String(50), nullable=False, default='pending'),  # 'pending', 'resolved', 'failed'
        sa.Column('resolution_data', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('conflict_id'),
        sa.ForeignKeyConstraint(['profile_id'], ['instrument_registry.subscription_profiles.profile_id']),
        sa.Index('idx_subscription_conflicts_profile_id', 'profile_id'),
        sa.Index('idx_subscription_conflicts_user_id', 'user_id'),
        sa.Index('idx_subscription_conflicts_status', 'status'),
        sa.Index('idx_subscription_conflicts_type', 'conflict_type'),
        schema='instrument_registry'
    )
    
    # Create user_subscription_limits table
    op.create_table(
        'user_subscription_limits',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('user_id', sa.String(50), nullable=False),
        sa.Column('subscription_type', sa.String(50), nullable=False),
        sa.Column('max_subscriptions', sa.Integer(), nullable=False),
        sa.Column('max_instruments_per_subscription', sa.Integer(), nullable=False),
        sa.Column('current_count', sa.Integer(), nullable=False, default=0),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'subscription_type'),
        sa.Index('idx_user_subscription_limits_user_id', 'user_id'),
        sa.Index('idx_user_subscription_limits_type', 'subscription_type'),
        schema='instrument_registry'
    )


def downgrade() -> None:
    """Drop subscription profile management tables"""
    
    # Drop tables in reverse order (due to foreign keys)
    op.drop_table('user_subscription_limits', schema='instrument_registry')
    op.drop_table('subscription_conflicts', schema='instrument_registry')
    op.drop_table('subscription_audit_log', schema='instrument_registry')
    op.drop_table('subscription_profiles', schema='instrument_registry')