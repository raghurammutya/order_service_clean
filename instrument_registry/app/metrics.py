"""
Prometheus Metrics Module for Instrument Registry

Centralized metrics to avoid circular imports.
"""

from prometheus_client import Counter, Histogram, Gauge

# Subscription planner metrics
subscription_plans_created_total = Counter(
    'instrument_registry_subscription_plans_created_total',
    'Total subscription plans created',
    ['optimization_level', 'filtering_strictness', 'status']
)

subscription_plan_descriptions_generated_total = Counter(
    'instrument_registry_subscription_plan_descriptions_generated_total',
    'Total subscription plan descriptions generated',
    ['description_level', 'status']
)

subscription_plan_cache_operations_total = Counter(
    'instrument_registry_subscription_plan_cache_operations_total',
    'Total subscription plan cache operations',
    ['operation', 'status']
)

subscription_plan_conflicts_total = Counter(
    'instrument_registry_subscription_plan_conflicts_total',
    'Total subscription plan conflicts',
    ['conflict_type', 'resolution']
)

subscription_plan_optimization_duration_seconds = Histogram(
    'instrument_registry_subscription_plan_optimization_duration_seconds',
    'Duration of subscription plan optimization',
    ['optimization_level']
)

subscription_plans_active = Gauge(
    'instrument_registry_subscription_plans_active',
    'Number of active subscription plans',
    ['user_id', 'subscription_type']
)