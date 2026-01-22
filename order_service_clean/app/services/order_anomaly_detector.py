"""
Order Anomaly Detection Service

Extends the existing audit service with real-time anomaly detection
for order submissions and trading patterns.
"""

import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class AnomalyType(str, Enum):
    """Types of anomalies detected"""
    HIGH_FREQUENCY_ORDERS = "high_frequency_orders"
    LARGE_ORDER_SIZE = "large_order_size"
    UNUSUAL_SYMBOLS = "unusual_symbols"
    OFF_HOURS_ACTIVITY = "off_hours_activity"
    RAPID_CANCELLATIONS = "rapid_cancellations"
    SUSPICIOUS_PATTERNS = "suspicious_patterns"


@dataclass
class AnomalyAlert:
    """Anomaly detection alert"""
    anomaly_type: AnomalyType
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    user_id: Optional[str]
    trading_account_id: Optional[str]
    event_count: int
    time_window: str
    detection_time: datetime
    metadata: Dict[str, Any]


class OrderAnomalyDetector:
    """
    Real-time anomaly detection for order activities.
    
    Works in conjunction with the existing OrderAuditService to detect
    suspicious trading patterns and potential security threats.
    """

    def __init__(self):
        # Ring buffers for recent activity tracking
        self.user_activities = defaultdict(lambda: deque(maxlen=1000))  # Per user
        self.account_activities = defaultdict(lambda: deque(maxlen=1000))  # Per account
        self.global_activities = deque(maxlen=10000)  # System-wide
        
        # Anomaly detection thresholds
        self.thresholds = {
            "max_orders_per_minute": 30,
            "max_orders_per_hour": 500,
            "large_order_quantity": 50000,
            "large_order_value": 1000000,  # 10L value
            "max_cancellations_per_minute": 20,
            "max_symbols_per_day": 15,
            "off_hours_sensitivity": "medium"  # low, medium, high
        }

    async def analyze_order_event(
        self,
        event_type: str,  # "placed", "modified", "cancelled"
        order_data: Dict[str, Any],
        user_id: str,
        trading_account_id: str,
        service_identity: str,
        request_id: str,
        ip_address: Optional[str] = None
    ) -> List[AnomalyAlert]:
        """
        Analyze an order event for anomalies.
        
        Args:
            event_type: Type of order event
            order_data: Order data dictionary
            user_id: User ID
            trading_account_id: Trading account ID
            service_identity: Service that initiated the order
            request_id: Request ID for correlation
            ip_address: Client IP address
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        # Create activity record
        activity = {
            "timestamp": datetime.now(),
            "event_type": event_type,
            "order_data": order_data,
            "user_id": user_id,
            "trading_account_id": trading_account_id,
            "service_identity": service_identity,
            "request_id": request_id,
            "ip_address": ip_address
        }
        
        # Store activity
        self.user_activities[user_id].append(activity)
        self.account_activities[trading_account_id].append(activity)
        self.global_activities.append(activity)
        
        # Run anomaly detections
        if event_type == "placed":
            anomalies.extend(await self._detect_order_placement_anomalies(activity))
        elif event_type == "cancelled":
            anomalies.extend(await self._detect_cancellation_anomalies(activity))
        
        # Always check for general patterns
        anomalies.extend(await self._detect_general_anomalies(activity))
        
        # Log anomalies
        for anomaly in anomalies:
            await self._log_anomaly(anomaly)
        
        return anomalies

    async def _detect_order_placement_anomalies(self, activity: Dict[str, Any]) -> List[AnomalyAlert]:
        """Detect anomalies in order placement"""
        anomalies = []
        user_id = activity["user_id"]
        order_data = activity["order_data"]
        now = activity["timestamp"]
        
        user_recent = list(self.user_activities[user_id])
        
        # High frequency order detection
        recent_orders = [
            a for a in user_recent
            if a["event_type"] == "placed"
            and (now - a["timestamp"]) <= timedelta(minutes=1)
        ]
        
        if len(recent_orders) > self.thresholds["max_orders_per_minute"]:
            anomalies.append(AnomalyAlert(
                anomaly_type=AnomalyType.HIGH_FREQUENCY_ORDERS,
                severity="HIGH",
                description=f"User placed {len(recent_orders)} orders in 1 minute",
                user_id=user_id,
                trading_account_id=activity["trading_account_id"],
                event_count=len(recent_orders),
                time_window="1 minute",
                detection_time=now,
                metadata={
                    "recent_orders": len(recent_orders),
                    "threshold": self.thresholds["max_orders_per_minute"],
                    "request_id": activity["request_id"]
                }
            ))
        
        # Large order size detection
        quantity = order_data.get("quantity", 0)
        price = order_data.get("price", 0)
        estimated_value = quantity * price
        
        if quantity > self.thresholds["large_order_quantity"]:
            severity = "HIGH" if quantity > self.thresholds["large_order_quantity"] * 2 else "MEDIUM"
            anomalies.append(AnomalyAlert(
                anomaly_type=AnomalyType.LARGE_ORDER_SIZE,
                severity=severity,
                description=f"Large order quantity: {quantity:,} units",
                user_id=user_id,
                trading_account_id=activity["trading_account_id"],
                event_count=1,
                time_window="single event",
                detection_time=now,
                metadata={
                    "quantity": quantity,
                    "estimated_value": estimated_value,
                    "symbol": order_data.get("symbol"),
                    "request_id": activity["request_id"]
                }
            ))
        
        if estimated_value > self.thresholds["large_order_value"]:
            anomalies.append(AnomalyAlert(
                anomaly_type=AnomalyType.LARGE_ORDER_SIZE,
                severity="HIGH",
                description=f"Large order value: ₹{estimated_value:,.2f}",
                user_id=user_id,
                trading_account_id=activity["trading_account_id"],
                event_count=1,
                time_window="single event",
                detection_time=now,
                metadata={
                    "estimated_value": estimated_value,
                    "quantity": quantity,
                    "price": price,
                    "symbol": order_data.get("symbol"),
                    "request_id": activity["request_id"]
                }
            ))
        
        # Unusual symbols detection
        daily_activities = [
            a for a in user_recent
            if a["event_type"] == "placed"
            and (now - a["timestamp"]) <= timedelta(hours=24)
        ]
        
        symbols_today = set(a["order_data"].get("symbol") for a in daily_activities)
        symbols_today.discard(None)  # Remove None values
        
        if len(symbols_today) > self.thresholds["max_symbols_per_day"]:
            anomalies.append(AnomalyAlert(
                anomaly_type=AnomalyType.UNUSUAL_SYMBOLS,
                severity="MEDIUM",
                description=f"User trading {len(symbols_today)} different symbols today",
                user_id=user_id,
                trading_account_id=activity["trading_account_id"],
                event_count=len(daily_activities),
                time_window="24 hours",
                detection_time=now,
                metadata={
                    "symbols_count": len(symbols_today),
                    "symbols_list": list(symbols_today)[:10],  # First 10 symbols
                    "total_orders": len(daily_activities),
                    "request_id": activity["request_id"]
                }
            ))
        
        return anomalies

    async def _detect_cancellation_anomalies(self, activity: Dict[str, Any]) -> List[AnomalyAlert]:
        """Detect anomalies in order cancellations"""
        anomalies = []
        user_id = activity["user_id"]
        now = activity["timestamp"]
        
        user_recent = list(self.user_activities[user_id])
        
        # Rapid cancellation detection
        recent_cancellations = [
            a for a in user_recent
            if a["event_type"] == "cancelled"
            and (now - a["timestamp"]) <= timedelta(minutes=1)
        ]
        
        if len(recent_cancellations) > self.thresholds["max_cancellations_per_minute"]:
            anomalies.append(AnomalyAlert(
                anomaly_type=AnomalyType.RAPID_CANCELLATIONS,
                severity="MEDIUM",
                description=f"User cancelled {len(recent_cancellations)} orders in 1 minute",
                user_id=user_id,
                trading_account_id=activity["trading_account_id"],
                event_count=len(recent_cancellations),
                time_window="1 minute",
                detection_time=now,
                metadata={
                    "recent_cancellations": len(recent_cancellations),
                    "threshold": self.thresholds["max_cancellations_per_minute"],
                    "request_id": activity["request_id"]
                }
            ))
        
        return anomalies

    async def _detect_general_anomalies(self, activity: Dict[str, Any]) -> List[AnomalyAlert]:
        """Detect general trading pattern anomalies"""
        anomalies = []
        now = activity["timestamp"]
        user_id = activity["user_id"]
        
        # Off-hours activity detection
        current_hour = now.hour
        
        # Indian market hours: 9 AM to 3:30 PM (IST)
        if current_hour < 9 or current_hour > 15:
            # Only flag as anomaly for order placement during off hours
            if activity["event_type"] == "placed":
                severity = "LOW" if self.thresholds["off_hours_sensitivity"] == "low" else "MEDIUM"
                anomalies.append(AnomalyAlert(
                    anomaly_type=AnomalyType.OFF_HOURS_ACTIVITY,
                    severity=severity,
                    description=f"Order placed during off-market hours: {now.strftime('%H:%M IST')}",
                    user_id=user_id,
                    trading_account_id=activity["trading_account_id"],
                    event_count=1,
                    time_window="off-hours",
                    detection_time=now,
                    metadata={
                        "order_time": now.strftime("%H:%M"),
                        "market_status": "closed",
                        "request_id": activity["request_id"]
                    }
                ))
        
        return anomalies

    async def _log_anomaly(self, anomaly: AnomalyAlert):
        """Log detected anomaly with appropriate severity"""
        anomaly_data = {
            "anomaly_detection": {
                "anomaly_type": anomaly.anomaly_type.value,
                "severity": anomaly.severity,
                "description": anomaly.description,
                "user_id": anomaly.user_id,
                "trading_account_id": anomaly.trading_account_id,
                "event_count": anomaly.event_count,
                "time_window": anomaly.time_window,
                "detection_time": anomaly.detection_time.isoformat(),
                "metadata": anomaly.metadata
            }
        }
        
        if anomaly.severity in ["HIGH", "CRITICAL"]:
            logger.critical(f"ORDER_ANOMALY_CRITICAL: {json.dumps(anomaly_data)}")
        elif anomaly.severity == "MEDIUM":
            logger.warning(f"ORDER_ANOMALY_WARNING: {json.dumps(anomaly_data)}")
        else:
            logger.info(f"ORDER_ANOMALY_INFO: {json.dumps(anomaly_data)}")

    def get_anomaly_statistics(self) -> Dict[str, Any]:
        """Get anomaly detection statistics for monitoring"""
        total_users_tracked = len(self.user_activities)
        total_accounts_tracked = len(self.account_activities)
        total_activities = len(self.global_activities)
        
        return {
            "tracking_stats": {
                "users_tracked": total_users_tracked,
                "accounts_tracked": total_accounts_tracked,
                "total_activities": total_activities
            },
            "thresholds": self.thresholds,
            "last_updated": datetime.now().isoformat()
        }

    def update_thresholds(self, new_thresholds: Dict[str, Any]):
        """Update anomaly detection thresholds"""
        self.thresholds.update(new_thresholds)
        logger.info(f"Updated anomaly detection thresholds: {new_thresholds}")


# Global instance
_anomaly_detector: Optional[OrderAnomalyDetector] = None


def get_anomaly_detector() -> OrderAnomalyDetector:
    """Get the global anomaly detector instance"""
    global _anomaly_detector
    if _anomaly_detector is None:
        _anomaly_detector = OrderAnomalyDetector()
    return _anomaly_detector


# Convenience functions for common operations
async def detect_order_anomalies(
    event_type: str,
    order_data: Dict[str, Any],
    user_id: str,
    trading_account_id: str,
    service_identity: str,
    request_id: str,
    ip_address: Optional[str] = None
) -> List[AnomalyAlert]:
    """Detect anomalies for an order event"""
    detector = get_anomaly_detector()
    return await detector.analyze_order_event(
        event_type=event_type,
        order_data=order_data,
        user_id=user_id,
        trading_account_id=trading_account_id,
        service_identity=service_identity,
        request_id=request_id,
        ip_address=ip_address
    )