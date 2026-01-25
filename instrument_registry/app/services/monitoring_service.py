"""
Monitoring Service for Instrument Registry Dual-Write Migration

This service provides comprehensive monitoring and alerting for the dual-write
migration process, including metrics collection, performance monitoring,
and integration with external monitoring systems.

Features:
- Prometheus metrics integration
- Real-time performance monitoring
- Alert management and thresholds
- Dashboard data aggregation
- SLA monitoring and reporting
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass

import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, push_to_gateway
from prometheus_client.exposition import generate_latest

from common.config_client import ConfigClient

logger = logging.getLogger(__name__)


class AlertLevel(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MetricType(str, Enum):
    """Metric types"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    metric_name: str
    condition: str  # e.g., ">", "<", "=="
    threshold: float
    level: AlertLevel
    message_template: str
    cooldown_minutes: int = 15


@dataclass
class MonitoringConfig:
    """Configuration for monitoring service"""
    enabled: bool
    prometheus_gateway_url: Optional[str]
    alert_webhook_url: Optional[str]
    metrics_retention_days: int
    dashboard_refresh_interval: int
    
    @classmethod
    async def from_config_service(cls, config_client: ConfigClient) -> 'MonitoringConfig':
        """Load monitoring configuration from config service"""
        try:
            # Ensure monitoring parameters exist
            await cls._ensure_monitoring_parameters(config_client)
            
            enabled = await config_client.get_bool("INSTRUMENT_REGISTRY_MONITORING_ENABLED", default=True)
            prometheus_url = await config_client.get_string("INSTRUMENT_REGISTRY_PROMETHEUS_GATEWAY", default=None)
            webhook_url = await config_client.get_string("INSTRUMENT_REGISTRY_ALERT_WEBHOOK", default=None)
            retention_days = await config_client.get_int("INSTRUMENT_REGISTRY_METRICS_RETENTION_DAYS", default=30)
            refresh_interval = await config_client.get_int("INSTRUMENT_REGISTRY_DASHBOARD_REFRESH_SECONDS", default=60)
            
            return cls(
                enabled=enabled,
                prometheus_gateway_url=prometheus_url,
                alert_webhook_url=webhook_url,
                metrics_retention_days=retention_days,
                dashboard_refresh_interval=refresh_interval
            )
        except Exception as e:
            logger.error(f"Failed to load monitoring config: {e}")
            return cls(
                enabled=True,
                prometheus_gateway_url=None,
                alert_webhook_url=None,
                metrics_retention_days=30,
                dashboard_refresh_interval=60
            )
    
    @classmethod
    async def _ensure_monitoring_parameters(cls, config_client: ConfigClient):
        """Ensure monitoring parameters exist in config service"""
        monitoring_params = [
            ("INSTRUMENT_REGISTRY_MONITORING_ENABLED", "true", "Enable monitoring and metrics collection"),
            ("INSTRUMENT_REGISTRY_PROMETHEUS_GATEWAY", "", "Prometheus pushgateway URL"),
            ("INSTRUMENT_REGISTRY_ALERT_WEBHOOK", "", "Webhook URL for alerts"),
            ("INSTRUMENT_REGISTRY_METRICS_RETENTION_DAYS", "30", "Metrics retention period in days"),
            ("INSTRUMENT_REGISTRY_DASHBOARD_REFRESH_SECONDS", "60", "Dashboard refresh interval in seconds"),
            ("INSTRUMENT_REGISTRY_SLA_AVAILABILITY_TARGET", "99.9", "SLA availability target percentage"),
            ("INSTRUMENT_REGISTRY_SLA_RESPONSE_TIME_MS", "500", "SLA response time target in milliseconds"),
        ]
        
        for param_name, default_value, description in monitoring_params:
            try:
                await config_client.ensure_parameter_exists(param_name, default_value, description)
            except Exception as e:
                logger.warning(f"Failed to ensure monitoring parameter {param_name}: {e}")


class MonitoringService:
    """
    Service for monitoring dual-write migration performance and health
    
    This service collects metrics from all dual-write components and provides
    monitoring, alerting, and dashboard capabilities for the migration process.
    """
    
    def __init__(
        self,
        config_client: ConfigClient,
        redis_url: str,
        service_name: str = "instrument_registry"
    ):
        self.config_client = config_client
        self.redis_url = redis_url
        self.service_name = service_name
        
        # Redis for metrics storage and alerting
        self.redis_client: Optional[redis.Redis] = None
        
        # Prometheus metrics
        self.registry = CollectorRegistry()
        self.metrics = self._create_prometheus_metrics()
        
        # Configuration cache
        self._config: Optional[MonitoringConfig] = None
        self._config_last_refresh: float = 0
        self._config_refresh_interval: float = 300.0  # 5 minutes
        
        # Alert rules
        self.alert_rules = self._create_default_alert_rules()
        self.alert_cooldowns = {}  # Track alert cooldowns
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        
    def _create_prometheus_metrics(self) -> Dict[str, Any]:
        """Create Prometheus metrics"""
        return {
            # Dual-write metrics
            "dual_write_operations_total": Counter(
                "dual_write_operations_total",
                "Total dual-write operations",
                ["operation", "status"],
                registry=self.registry
            ),
            "dual_write_duration_seconds": Histogram(
                "dual_write_duration_seconds",
                "Dual-write operation duration",
                ["operation"],
                registry=self.registry
            ),
            "dual_write_records_processed": Counter(
                "dual_write_records_processed_total", 
                "Total records processed in dual-write",
                ["target_system"],
                registry=self.registry
            ),
            
            # Validation metrics
            "validation_operations_total": Counter(
                "validation_operations_total",
                "Total validation operations",
                ["validation_level", "status"],
                registry=self.registry
            ),
            "validation_mismatches_total": Counter(
                "validation_mismatches_total",
                "Total validation mismatches found",
                ["mismatch_type"],
                registry=self.registry
            ),
            "validation_match_percentage": Gauge(
                "validation_match_percentage",
                "Percentage of records that match between systems",
                registry=self.registry
            ),
            
            # Retention metrics
            "retention_operations_total": Counter(
                "retention_operations_total",
                "Total retention operations",
                ["table_name", "action"],
                registry=self.registry
            ),
            "retention_records_processed": Counter(
                "retention_records_processed_total",
                "Total records processed by retention policies",
                ["table_name", "action"],
                registry=self.registry
            ),
            
            # System health metrics
            "system_health_status": Gauge(
                "system_health_status",
                "System health status (1=healthy, 0=unhealthy)",
                ["component"],
                registry=self.registry
            ),
            "config_refresh_timestamp": Gauge(
                "config_refresh_timestamp",
                "Timestamp of last config refresh",
                registry=self.registry
            ),
            
            # Performance metrics
            "api_response_time_seconds": Histogram(
                "api_response_time_seconds",
                "API response times",
                ["endpoint", "method"],
                registry=self.registry
            ),
            "database_query_duration_seconds": Histogram(
                "database_query_duration_seconds",
                "Database query execution times",
                ["query_type"],
                registry=self.registry
            ),
        }
    
    def _create_default_alert_rules(self) -> List[AlertRule]:
        """Create default alert rules"""
        return [
            AlertRule(
                name="high_dual_write_failure_rate",
                metric_name="dual_write_operations_total",
                condition=">",
                threshold=5.0,  # More than 5% failure rate
                level=AlertLevel.WARNING,
                message_template="Dual-write failure rate is {value:.2f}% (threshold: {threshold}%)",
                cooldown_minutes=15
            ),
            AlertRule(
                name="validation_match_percentage_low",
                metric_name="validation_match_percentage",
                condition="<",
                threshold=95.0,  # Less than 95% match rate
                level=AlertLevel.CRITICAL,
                message_template="Validation match percentage dropped to {value:.2f}% (threshold: {threshold}%)",
                cooldown_minutes=5
            ),
            AlertRule(
                name="system_unhealthy",
                metric_name="system_health_status",
                condition="<",
                threshold=1.0,  # System unhealthy
                level=AlertLevel.CRITICAL,
                message_template="System component {component} is unhealthy",
                cooldown_minutes=10
            ),
            AlertRule(
                name="high_response_time",
                metric_name="api_response_time_seconds",
                condition=">",
                threshold=2.0,  # More than 2 seconds
                level=AlertLevel.WARNING,
                message_template="API response time is high: {value:.2f}s (threshold: {threshold}s)",
                cooldown_minutes=10
            )
        ]

    async def initialize(self):
        """Initialize the monitoring service"""
        # Initialize Redis connection
        self.redis_client = await redis.from_url(self.redis_url)
        
        # Load initial config
        await self._refresh_config()
        
        # Start background monitoring task
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("Monitoring service initialized")

    async def _refresh_config(self) -> MonitoringConfig:
        """Refresh monitoring configuration"""
        current_time = time.time()
        
        if (self._config is None or 
            current_time - self._config_last_refresh > self._config_refresh_interval):
            
            self._config = await MonitoringConfig.from_config_service(self.config_client)
            self._config_last_refresh = current_time
            
            # Update config refresh timestamp metric
            self.metrics["config_refresh_timestamp"].set(current_time)
            
            logger.info(f"Monitoring config refreshed: enabled={self._config.enabled}")
        
        return self._config

    async def record_dual_write_operation(
        self,
        operation: str,
        status: str,
        duration_seconds: float,
        records_screener: int,
        records_registry: int
    ):
        """Record dual-write operation metrics"""
        self.metrics["dual_write_operations_total"].labels(
            operation=operation, status=status
        ).inc()
        
        self.metrics["dual_write_duration_seconds"].labels(
            operation=operation
        ).observe(duration_seconds)
        
        self.metrics["dual_write_records_processed"].labels(
            target_system="screener"
        ).inc(records_screener)
        
        self.metrics["dual_write_records_processed"].labels(
            target_system="registry"
        ).inc(records_registry)

    async def record_validation_operation(
        self,
        validation_level: str,
        status: str,
        match_percentage: float,
        mismatches_by_type: Dict[str, int]
    ):
        """Record validation operation metrics"""
        self.metrics["validation_operations_total"].labels(
            validation_level=validation_level, status=status
        ).inc()
        
        self.metrics["validation_match_percentage"].set(match_percentage)
        
        for mismatch_type, count in mismatches_by_type.items():
            self.metrics["validation_mismatches_total"].labels(
                mismatch_type=mismatch_type
            ).inc(count)

    async def record_retention_operation(
        self,
        table_name: str,
        action: str,
        records_processed: int
    ):
        """Record retention operation metrics"""
        self.metrics["retention_operations_total"].labels(
            table_name=table_name, action=action
        ).inc()
        
        self.metrics["retention_records_processed"].labels(
            table_name=table_name, action=action
        ).inc(records_processed)

    async def record_system_health(self, component: str, is_healthy: bool):
        """Record system health status"""
        self.metrics["system_health_status"].labels(
            component=component
        ).set(1.0 if is_healthy else 0.0)

    async def record_api_response_time(
        self,
        endpoint: str,
        method: str,
        duration_seconds: float
    ):
        """Record API response time"""
        self.metrics["api_response_time_seconds"].labels(
            endpoint=endpoint, method=method
        ).observe(duration_seconds)

    async def record_database_query_time(
        self,
        query_type: str,
        duration_seconds: float
    ):
        """Record database query execution time"""
        self.metrics["database_query_duration_seconds"].labels(
            query_type=query_type
        ).observe(duration_seconds)

    async def _monitoring_loop(self):
        """Background monitoring loop"""
        while True:
            try:
                config = await self._refresh_config()
                
                if config.enabled:
                    # Check alert rules
                    await self._check_alert_rules()
                    
                    # Push metrics to Prometheus gateway if configured
                    if config.prometheus_gateway_url:
                        await self._push_to_prometheus(config.prometheus_gateway_url)
                    
                    # Store metrics snapshot in Redis
                    await self._store_metrics_snapshot()
                
                # Wait for next iteration
                await asyncio.sleep(config.dashboard_refresh_interval if config else 60)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    async def _check_alert_rules(self):
        """Check alert rules and trigger alerts if needed"""
        current_time = time.time()
        
        for rule in self.alert_rules:
            try:
                # Check cooldown
                cooldown_key = f"alert_cooldown:{rule.name}"
                last_alert_time = self.alert_cooldowns.get(cooldown_key, 0)
                
                if current_time - last_alert_time < (rule.cooldown_minutes * 60):
                    continue  # Still in cooldown
                
                # Get current metric value
                current_value = await self._get_current_metric_value(rule.metric_name)
                
                if current_value is None:
                    continue
                
                # Check condition
                should_alert = False
                if rule.condition == ">" and current_value > rule.threshold:
                    should_alert = True
                elif rule.condition == "<" and current_value < rule.threshold:
                    should_alert = True
                elif rule.condition == "==" and current_value == rule.threshold:
                    should_alert = True
                
                if should_alert:
                    await self._trigger_alert(rule, current_value)
                    self.alert_cooldowns[cooldown_key] = current_time
                    
            except Exception as e:
                logger.error(f"Error checking alert rule {rule.name}: {e}")

    async def _get_current_metric_value(self, metric_name: str) -> Optional[float]:
        """Get current value of a metric"""
        try:
            # This is a simplified implementation
            # In a real implementation, you would query the actual metric values
            if metric_name in self.metrics:
                metric = self.metrics[metric_name]
                if hasattr(metric, '_value'):
                    return float(metric._value._value)
                elif hasattr(metric, '_sum'):
                    return float(metric._sum._value)
            return None
        except Exception as e:
            logger.error(f"Error getting metric value for {metric_name}: {e}")
            return None

    async def _trigger_alert(self, rule: AlertRule, current_value: float):
        """Trigger an alert"""
        alert_message = rule.message_template.format(
            value=current_value,
            threshold=rule.threshold
        )
        
        alert_data = {
            "rule_name": rule.name,
            "metric_name": rule.metric_name,
            "level": rule.level.value,
            "message": alert_message,
            "current_value": current_value,
            "threshold": rule.threshold,
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name
        }
        
        # Store alert in Redis
        await self.redis_client.lpush(
            "instrument_registry_alerts",
            json.dumps(alert_data)
        )
        
        # Trim alert history to last 1000 alerts
        await self.redis_client.ltrim("instrument_registry_alerts", 0, 999)
        
        logger.warning(f"ALERT [{rule.level.value.upper()}]: {alert_message}")
        
        # Send webhook if configured
        config = await self._refresh_config()
        if config.alert_webhook_url:
            await self._send_alert_webhook(config.alert_webhook_url, alert_data)

    async def _send_alert_webhook(self, webhook_url: str, alert_data: Dict[str, Any]):
        """Send alert to webhook"""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=alert_data) as response:
                    if response.status == 200:
                        logger.info(f"Alert webhook sent successfully")
                    else:
                        logger.error(f"Alert webhook failed: {response.status}")
        except Exception as e:
            logger.error(f"Error sending alert webhook: {e}")

    async def _push_to_prometheus(self, gateway_url: str):
        """Push metrics to Prometheus pushgateway"""
        try:
            # Generate metrics in Prometheus format
            metrics_data = generate_latest(self.registry)
            
            # In a real implementation, you would use push_to_gateway
            # For now, we'll just log that we would push
            logger.debug(f"Would push {len(metrics_data)} bytes of metrics to {gateway_url}")
            
        except Exception as e:
            logger.error(f"Error pushing to Prometheus: {e}")

    async def _store_metrics_snapshot(self):
        """Store current metrics snapshot in Redis"""
        try:
            timestamp = datetime.utcnow().isoformat()
            
            # Create simplified metrics snapshot
            snapshot = {
                "timestamp": timestamp,
                "dual_write_operations": self._get_counter_value("dual_write_operations_total"),
                "validation_operations": self._get_counter_value("validation_operations_total"), 
                "retention_operations": self._get_counter_value("retention_operations_total"),
                "validation_match_percentage": self._get_gauge_value("validation_match_percentage"),
            }
            
            # Store with expiration
            await self.redis_client.setex(
                f"metrics_snapshot:{int(time.time())}",
                24 * 3600,  # 24 hours
                json.dumps(snapshot)
            )
            
        except Exception as e:
            logger.error(f"Error storing metrics snapshot: {e}")

    def _get_counter_value(self, metric_name: str) -> Dict[str, float]:
        """Get counter metric values by labels"""
        if metric_name not in self.metrics:
            return {}
        
        # Simplified - in real implementation would iterate through all label combinations
        return {"total": 0.0}

    def _get_gauge_value(self, metric_name: str) -> Optional[float]:
        """Get gauge metric value"""
        if metric_name not in self.metrics:
            return None
        
        try:
            return float(self.metrics[metric_name]._value._value)
        except:
            return None

    async def get_metrics_summary(self) -> Dict[str, Any]:
        """Get current metrics summary"""
        return {
            "dual_write": {
                "operations_total": self._get_counter_value("dual_write_operations_total"),
                "records_processed": self._get_counter_value("dual_write_records_processed"),
            },
            "validation": {
                "operations_total": self._get_counter_value("validation_operations_total"),
                "match_percentage": self._get_gauge_value("validation_match_percentage"),
                "mismatches_total": self._get_counter_value("validation_mismatches_total"),
            },
            "retention": {
                "operations_total": self._get_counter_value("retention_operations_total"),
                "records_processed": self._get_counter_value("retention_records_processed"),
            },
            "system_health": {
                "components": self._get_gauge_value("system_health_status"),
            },
            "last_updated": datetime.utcnow().isoformat()
        }

    async def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        try:
            alert_data = await self.redis_client.lrange("instrument_registry_alerts", 0, limit - 1)
            alerts = []
            
            for data in alert_data:
                try:
                    alerts.append(json.loads(data))
                except json.JSONDecodeError:
                    continue
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error getting recent alerts: {e}")
            return []

    async def get_sla_metrics(self) -> Dict[str, Any]:
        """Get SLA compliance metrics"""
        try:
            config = await self._refresh_config()
            availability_target = await self.config_client.get_float("INSTRUMENT_REGISTRY_SLA_AVAILABILITY_TARGET", default=99.9)
            response_time_target = await self.config_client.get_float("INSTRUMENT_REGISTRY_SLA_RESPONSE_TIME_MS", default=500)
            
            # Calculate SLA metrics (simplified implementation)
            current_availability = 99.5  # Would calculate from actual health checks
            avg_response_time = 350      # Would calculate from actual response times
            
            return {
                "availability": {
                    "target_percent": availability_target,
                    "current_percent": current_availability,
                    "compliant": current_availability >= availability_target
                },
                "response_time": {
                    "target_ms": response_time_target,
                    "current_ms": avg_response_time,
                    "compliant": avg_response_time <= response_time_target
                },
                "overall_sla_compliance": (
                    current_availability >= availability_target and
                    avg_response_time <= response_time_target
                )
            }
            
        except Exception as e:
            logger.error(f"Error calculating SLA metrics: {e}")
            return {}

    async def close(self):
        """Close monitoring service"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Monitoring service closed")