#!/usr/bin/env python3
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from sqlalchemy import create_engine, text
import os

# Simple HTTP server for demonstration
class OrderServiceHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, db_engine=None, **kwargs):
        self.db_engine = db_engine
        super().__init__(*args, **kwargs)
        
    def do_GET(self):
        try:
            if self.path == '/':
                response = {
                    "service": "order_service",
                    "status": "operational", 
                    "pid": os.getpid(),
                    "timestamp": datetime.now().isoformat()
                }
            elif self.path == '/health':
                # Test database connection
                with self.db_engine.begin() as conn:
                    conn.execute(text("SELECT 1"))
                response = {
                    "status": "healthy",
                    "database": "connected",
                    "pid": os.getpid(),
                    "timestamp": datetime.now().isoformat()
                }
            elif self.path == '/api/v1/stats':
                # Live database query
                with self.db_engine.begin() as conn:
                    result = conn.execute(text("""
                        SELECT 
                            COUNT(*) as orders,
                            (SELECT COUNT(*) FROM order_service.positions WHERE is_open = true) as positions
                        FROM order_service.orders
                    """))
                    row = result.fetchone()
                    response = {
                        "total_orders": row[0],
                        "open_positions": row[1],
                        "timestamp": datetime.now().isoformat(),
                        "pid": os.getpid()
                    }
            else:
                self.send_error(404)
                return
                
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response, indent=2).encode())
            
        except Exception as e:
            self.send_error(500, f"Server error: {str(e)}")
            
    def log_message(self, format, *args):
        print(f"{datetime.now().isoformat()} - {format % args}")

def create_handler(db_engine):
    def handler(*args, **kwargs):
        OrderServiceHandler(*args, db_engine=db_engine, **kwargs)
    return handler

if __name__ == "__main__":
    # Connect to database
    db_engine = create_engine("postgresql://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod")
    
    # Test database
    with db_engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM order_service.orders"))
        print(f"Database connected: {result.scalar()} orders found")
    
    # Start server
    server = HTTPServer(('0.0.0.0', 8004), lambda *args: OrderServiceHandler(*args, db_engine=db_engine))
    print(f"Order Service running on port 8004 (PID: {os.getpid()})")
    server.serve_forever()