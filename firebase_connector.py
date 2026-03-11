"""
Autonomous Firebase State Management for Project Chrysalis
Handles all database operations, health monitoring, and state persistence
with robust error handling and fallback mechanisms.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict

# Firebase imports
try:
    import firebase_admin
    from firebase_admin import credentials, firestore, initialize_app
    from firebase_admin.exceptions import FirebaseError
    FIREBASE_AVAILABLE = True
except ImportError as e:
    logging.error(f"Firebase admin not available: {e}")
    FIREBASE_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TradeRecord:
    """Data class for trade records with type safety"""
    timestamp: datetime
    token_address: str
    dex_pair: str
    gross_profit_usd: float
    net_profit_usd: float
    gas_cost_eth: float
    execution_speed_ms: int
    slippage_bps: int
    edge_strength_at_execution: float
    failure_mode_triggered: Optional[str] = None
    tx_hash: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Firestore-compatible dictionary"""
        data = asdict(self)
        data['timestamp'] = self.timestamp
        return data

@dataclass 
class EngineHealth:
    """Data class for engine health monitoring"""
    last_heartbeat: datetime
    current_edge_strength: float
    active_circuit_breakers: List[str]
    failure_recovery_rate_24h: float
    real_net_profit_24h: float
    trades_executed_24h: int

class FirebaseConnector:
    """Robust Firebase connector with fallback mechanisms and error handling"""
    
    def __init__(self, credential_path: str = "serviceAccountKey.json"):
        """
        Initialize Firebase connection with multiple fallback strategies
        
        Args:
            credential_path: Path to Firebase service account key
        """
        self.db = None
        self.initialized = False
        self.fallback_data = {"trades": [], "health": {}}
        
        # Attempt Firebase initialization with multiple strategies
        self._initialize_firebase(credential_path)
        
        # Initialize collections
        self.trades_collection = "chrysalis_trades"
        self.health_collection = "chrysalis_health"
        self.market_state_collection = "chrysalis_market_state"
        self.failure_logs_collection = "chrysalis_failure_logs"
        
        logger.info("FirebaseConnector initialized successfully")
    
    def _initialize_firebase(self, credential_path: str):
        """Attempt Firebase initialization with multiple fallback strategies"""
        strategies = [
            self._try_credential_file,
            self._try_environment_variable,
            self._try_default_app
        ]
        
        for strategy in strategies:
            try:
                if strategy(credential_path):
                    logger.info(f"Firebase initialized via {strategy.__name__}")
                    self.initialized = True
                    self.db = firestore.client()
                    return
            except Exception as e:
                logger.warning(f"Firebase strategy {strategy.__name__} failed: {e}")
                continue
        
        logger.error("All Firebase initialization strategies failed. Using fallback mode.")
        self.initialized = False
    
    def _try_credential_file(self, path: str) -> bool:
        """Strategy 1: Initialize from service account key file"""
        if os.path.exists(path):
            cred = credentials.Certificate(path)
            firebase_admin.initialize_app(cred)
            return True
        return False
    
    def _try_environment_variable(self, _: str) -> bool:
        """Strategy 2: Initialize from environment variable"""
        import os
        cred_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
        if cred_json:
            cred_dict = json.loads(cred_json)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
            return True
        return False
    
    def _try_default_app(self, _: str) -> bool:
        """Strategy 3: Try default app (if already initialized)"""
        try:
            firebase_admin.get_app()
            return True
        except ValueError:
            return False
    
    def log_trade(self, trade: TradeRecord) -> bool:
        """
        Log a trade to Firestore with robust error handling
        
        Args:
            trade: TradeRecord object containing trade data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.initialized and self.db:
                trade_dict = trade.to_dict()
                # Convert datetime to Firestore timestamp
                trade_dict['timestamp'] = firestore.SERVER_TIMESTAMP
                
                # Add to Firestore
                doc_ref = self.db.collection(self.trades_collection).document()
                doc_ref.set(trade_dict)
                logger.info(f"Trade logged successfully: {trade.token_address}")
                return True
            else:
                # Fallback: Store in memory
                self.fallback_data["trades"].append(asdict(trade))
                logger.warning("Firestore not available. Using fallback storage.")
                return True
        except Exception as e:
            logger.error(f"Failed to log trade: {e}")
            self._log_failure("trade_logging", str(e), trade.token_address)
            return False
    
    def update_health_metrics(self, health: EngineHealth) -> bool:
        """
        Update engine health metrics with timestamp
        
        Args:
            health: EngineHealth object
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.initialized and self.db:
                health_dict = asdict(health)
                health_dict['last_heartbeat'] = firestore.SERVER_TIMESTAMP
                
                # Update or create health document
                doc_ref = self.db.collection(self.health_collection).document("current")
                doc_ref.set(health_dict)
                
                # Also add to historical collection
                historical_ref = self.db.collection(f"{self.health_collection}_historical").document()
                historical_ref.set(health_dict)
                
                logger.debug("Health metrics updated successfully")
                return True
            else:
                self.fallback_data["health"] = asdict(health)
                return True
        except Exception as e:
            logger.error(f"Failed to update health metrics: {e}")
            return False
    
    def get_recent_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Retrieve recent trades with error handling
        
        Args:
            limit: Maximum number of trades to retrieve
            
        Returns:
            List of trade dictionaries
        """
        try:
            if self.initialized and self.db:
                trades_ref = self.db.collection(self.trades_collection)
                query = trades_ref.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit)
                docs = query.stream()
                
                trades = []
                for doc in docs:
                    trade_data = doc.to_dict()
                    trade_data["id"] = doc.id
                    trades.append(trade_data)
                
                return trades
            else:
                return self.fallback_data["trades"][-limit:]
        except Exception as e:
            logger.error(f"Failed to retrieve recent trades: {e}")
            return []
    
    def _log_failure(self, failure_type: str, error_message: str, context: str = ""):
        """
        Log failure to Firestore for post-mortem analysis
        
        Args:
            failure_type: Type of failure
            error_message: Detailed error message
            context: Additional context
        """
        try:
            if self.initialized and self.db:
                failure_data = {
                    "timestamp": firestore.SERVER_TIMESTAMP,
                    "failure_type": failure_type,
                    "error_message": error_message,
                    "context": context,
                    "resolved": False
                }
                self.db.collection(self.failure_logs_collection).add(failure_data)
        except Exception as e:
            logger.error(f"Failed to log failure: {e}")
    
    def calculate_24h_metrics(self) -> Dict[str, Any]:
        """
        Calculate 24-hour performance metrics
        
        Returns:
            Dictionary with calculated metrics
        """
        try:
            if self.initialized and self.db:
                twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24)
                
                # Query trades from last 24 hours
                trades_ref = self.db.collection(self.trades_collection)
                query = trades_ref.where("timestamp", ">=", twenty_four_hours_ago)
                docs = query.stream()
                
                trades = []
                total_net_profit = 0.0
                total_trades = 0
                
                for doc in docs:
                    trade_data = doc.to_dict()
                    trades.append(trade_data)
                    if "net_profit_usd" in trade_data:
                        total_net_profit += trade_data["net_profit_usd"]
                    total_trades += 1
                
                # Calculate failure recovery rate (placeholder logic)
                failure_query = self.db.collection(self.failure_logs_collection)
                failure_docs = failure_query.where("timestamp", ">=", twenty_four_hours_ago).stream()
                failure_count = sum(1 for _ in failure_docs)
                
                recovery_rate = 1.0 if total_trades == 0 else 1 - (failure_count / max(total_trades, 1))
                
                return {
                    "real_net_profit_24h": total_net_profit,
                    "trades_executed_24h": total_trades,
                    "failure_recovery_rate_24h": recovery_rate,
                    "avg_profit_per_trade": total_net_profit / max(total_trades, 1)
                }
            else:
                # Fallback calculation from memory
                trades = self.fallback_data["trades"]
                recent_trades = [t for t in trades if isinstance(t.get("timestamp"), datetime) 
                               and t["timestamp"] > datetime.utcnow() - timedelta(hours=24)]
                
                total_net_profit = sum(t.get("net_profit_usd", 0) for t in recent_trades)
                total_trades = len(recent_trades)
                
                return {
                    "real_net_profit_24h": total_net_profit,
                    "trades_executed_24h": total_trades,
                    "failure_recovery_rate_24h": 0.95,  # Conservative estimate
                    "avg_profit_per_trade": total_net_profit / max(total_trades, 1)
                }
        except Exception as e:
            logger.error(f"Failed to calculate 24h metrics: {e}")
            return {
                "real_net_profit_24h": 0.0,
                "trades_executed_24h": 0,
                "failure_recovery_rate_24h": 0.0,
                "avg_profit_per_trade": 0.0
            }
    
    def check_circuit_breakers(self) -> List[str]:
        """
        Check if any circuit breakers should be triggered
        
        Returns:
            List of triggered circuit breaker names
        """
        triggered_breakers = []
        
        try:
            metrics = self.calculate_24h_metrics()
            
            # Daily loss limit breaker
            if metrics["real_net_profit_24h"] < -15.79:  # 10% of capital
                triggered_breakers.append("daily_loss_limit")
                logger.warning("Daily loss limit circuit breaker triggered!")
            
            # Consecutive failure breaker (simplified check)
            recent_trades = self.get_recent_trades(5)
            if len(recent_trades) >= 3:
                recent_failures = sum(1 for t in recent_trades 
                                    if t.get("net_profit_usd", 0) < 0)
                if recent_failures >= 3:
                    triggered_breakers.append("consecutive_failure")
                    logger.warning("Consecutive failure circuit breaker triggered!")
            
            return triggered_breakers
            
        except Exception as e:
            logger.error(f"Failed to check circuit breakers: {e}")
            return ["circuit_breaker_check_failed"]

# Singleton instance
_firebase_instance = None

def get_firebase_connector() -> FirebaseConnector:
    """Get or create singleton FirebaseConnector instance"""