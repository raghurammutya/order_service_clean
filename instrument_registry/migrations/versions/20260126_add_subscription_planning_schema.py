"""Add subscription planning schema

Revision ID: 20260126_add_subscription_planning
Revises: 20260125_initial_schema
Create Date: 2026-01-26 11:45:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = '20260126_add_subscription_planning'
down_revision = '20260125_initial_schema'
branch_labels = None
depends_on = None

def upgrade():
    """Add subscription planning tables with proper schema enforcement"""
    
    # Create subscription_plans table
    op.create_table(
        'subscription_plans',
        sa.Column('plan_id', sa.String(64), primary_key=True),
        sa.Column('user_id', sa.String(64), nullable=False, index=True),
        sa.Column('plan_name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text),
        sa.Column('subscription_type', sa.String(32), nullable=False),
        sa.Column('instruments', sa.Text, nullable=False),  # JSON array as text
        sa.Column('optimization_level', sa.String(32), nullable=False),
        sa.Column('filtering_strictness', sa.String(32), nullable=False),
        sa.Column('status', sa.String(32), nullable=False),
        sa.Column('estimated_cost', sa.Numeric(10, 4)),
        sa.Column('performance_metrics', sa.Text),  # JSON as text
        sa.Column('validation_results', sa.Text),  # JSON as text
        sa.Column('cache_key', sa.String(128), index=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True)),
        sa.Column('metadata', sa.Text),  # JSON as text
        schema='instrument_registry'
    )
    
    # Create indexes for subscription_plans
    op.create_index('ix_subscription_plans_user_type', 'subscription_plans', 
                   ['user_id', 'subscription_type'], schema='instrument_registry')
    op.create_index('ix_subscription_plans_status', 'subscription_plans', 
                   ['status'], schema='instrument_registry')
    op.create_index('ix_subscription_plans_created_at', 'subscription_plans', 
                   ['created_at'], schema='instrument_registry')
    op.create_index('ix_subscription_plans_expires_at', 'subscription_plans', 
                   ['expires_at'], schema='instrument_registry')
    
    # Add check constraints
    op.create_check_constraint(
        'ck_subscription_plans_subscription_type',
        'subscription_plans',
        "subscription_type IN ('live_feed', 'historical', 'alerts')",
        schema='instrument_registry'
    )
    
    op.create_check_constraint(
        'ck_subscription_plans_optimization_level',
        'subscription_plans',
        "optimization_level IN ('low', 'moderate', 'aggressive')",
        schema='instrument_registry'
    )
    
    op.create_check_constraint(
        'ck_subscription_plans_filtering_strictness',
        'subscription_plans',
        "filtering_strictness IN ('lenient', 'moderate', 'strict')",
        schema='instrument_registry'
    )
    
    op.create_check_constraint(
        'ck_subscription_plans_status',
        'subscription_plans',
        "status IN ('draft', 'optimized', 'validated', 'error')",
        schema='instrument_registry'
    )
    
    op.create_check_constraint(
        'ck_subscription_plans_estimated_cost',
        'subscription_plans',
        "estimated_cost >= 0",
        schema='instrument_registry'
    )
    
    # Create plan_descriptions table for caching descriptions
    op.create_table(
        'plan_descriptions',
        sa.Column('description_id', sa.String(64), primary_key=True),
        sa.Column('plan_id', sa.String(64), nullable=False, index=True),
        sa.Column('description_level', sa.String(32), nullable=False),
        sa.Column('description_data', sa.Text, nullable=False),  # JSON as text
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        schema='instrument_registry'
    )
    
    # Foreign key for plan_descriptions
    op.create_foreign_key(
        'fk_plan_descriptions_plan_id',
        'plan_descriptions', 'subscription_plans',
        ['plan_id'], ['plan_id'],
        source_schema='instrument_registry',
        referent_schema='instrument_registry',
        ondelete='CASCADE'
    )
    
    # Create indexes for plan_descriptions
    op.create_index('ix_plan_descriptions_plan_level', 'plan_descriptions', 
                   ['plan_id', 'description_level'], schema='instrument_registry')
    op.create_index('ix_plan_descriptions_expires_at', 'plan_descriptions', 
                   ['expires_at'], schema='instrument_registry')
    
    # Add check constraint for description_level
    op.create_check_constraint(
        'ck_plan_descriptions_description_level',
        'plan_descriptions',
        "description_level IN ('basic', 'detailed', 'comprehensive')",
        schema='instrument_registry'
    )
    
    # Create plan_optimization_metrics table for tracking optimization performance
    op.create_table(
        'plan_optimization_metrics',
        sa.Column('metric_id', sa.String(64), primary_key=True),
        sa.Column('plan_id', sa.String(64), nullable=False, index=True),
        sa.Column('optimization_level', sa.String(32), nullable=False),
        sa.Column('original_instrument_count', sa.Integer, nullable=False),
        sa.Column('optimized_instrument_count', sa.Integer, nullable=False),
        sa.Column('removed_instrument_count', sa.Integer, nullable=False),
        sa.Column('processing_time_ms', sa.Integer, nullable=False),
        sa.Column('memory_usage_mb', sa.Numeric(10, 2)),
        sa.Column('cache_hit', sa.Boolean, nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        schema='instrument_registry'
    )
    
    # Foreign key for plan_optimization_metrics
    op.create_foreign_key(
        'fk_plan_optimization_metrics_plan_id',
        'plan_optimization_metrics', 'subscription_plans',
        ['plan_id'], ['plan_id'],
        source_schema='instrument_registry',
        referent_schema='instrument_registry',
        ondelete='CASCADE'
    )
    
    # Create indexes for plan_optimization_metrics
    op.create_index('ix_plan_optimization_metrics_optimization_level', 'plan_optimization_metrics', 
                   ['optimization_level'], schema='instrument_registry')
    op.create_index('ix_plan_optimization_metrics_created_at', 'plan_optimization_metrics', 
                   ['created_at'], schema='instrument_registry')
    op.create_index('ix_plan_optimization_metrics_cache_hit', 'plan_optimization_metrics', 
                   ['cache_hit'], schema='instrument_registry')
    
    # Add check constraints for plan_optimization_metrics
    op.create_check_constraint(
        'ck_plan_optimization_metrics_optimization_level',
        'plan_optimization_metrics',
        "optimization_level IN ('low', 'moderate', 'aggressive')",
        schema='instrument_registry'
    )
    
    op.create_check_constraint(
        'ck_plan_optimization_metrics_counts',
        'plan_optimization_metrics',
        "original_instrument_count >= optimized_instrument_count AND removed_instrument_count >= 0",
        schema='instrument_registry'
    )
    
    op.create_check_constraint(
        'ck_plan_optimization_metrics_processing_time',
        'plan_optimization_metrics',
        "processing_time_ms >= 0",
        schema='instrument_registry'
    )

def downgrade():
    """Remove subscription planning tables"""
    
    # Drop tables in reverse order (respecting foreign key dependencies)
    op.drop_table('plan_optimization_metrics', schema='instrument_registry')
    op.drop_table('plan_descriptions', schema='instrument_registry')
    op.drop_table('subscription_plans', schema='instrument_registry')