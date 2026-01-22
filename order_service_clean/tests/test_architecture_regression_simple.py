"""
Architecture Compliance Regression Tests - Simplified Version

Tests key architectural changes without requiring full app setup:
- Import and syntax validation
- Basic class instantiation
- Schema compliance checks
- Configuration validation
"""

import pytest
import inspect
import ast
import re
from pathlib import Path


class TestCodeCompilation:
    """Test all new files compile without errors"""
    
    def test_service_clients_compile(self):
        """Test service client files compile without syntax errors"""
        import py_compile
        
        client_files = [
            "app/clients/strategy_service_client.py",
            "app/clients/portfolio_service_client.py", 
            "app/clients/account_service_client.py",
            "app/clients/analytics_service_client.py"
        ]
        
        for file_path in client_files:
            try:
                py_compile.compile(file_path, doraise=True)
            except py_compile.PyCompileError as e:
                pytest.fail(f"Syntax error in {file_path}: {e}")
    
    def test_security_files_compile(self):
        """Test security and monitoring files compile"""
        import py_compile
        
        security_files = [
            "app/security/internal_auth.py",
            "app/services/redis_usage_monitor.py",
            "app/services/order_anomaly_detector.py"
        ]
        
        for file_path in security_files:
            try:
                py_compile.compile(file_path, doraise=True)
            except py_compile.PyCompileError as e:
                pytest.fail(f"Syntax error in {file_path}: {e}")


class TestSchemaCompliance:
    """Test schema boundary compliance without database"""
    
    def test_no_direct_public_schema_access(self):
        """Test that service clients don't contain direct public.* table access"""
        client_files = [
            "app/clients/strategy_service_client.py",
            "app/clients/portfolio_service_client.py",
            "app/clients/account_service_client.py", 
            "app/clients/analytics_service_client.py"
        ]
        
        for file_path in client_files:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Remove all comments and docstrings to test only actual code
            # This removes single-line comments, docstrings, and documentation
            lines = content.split('\n')
            code_lines = []
            in_docstring = False
            docstring_delimiter = None
            
            for line in lines:
                stripped = line.strip()
                
                # Skip single-line comments
                if stripped.startswith('#'):
                    continue
                    
                # Handle docstrings
                if '"""' in line or "'''" in line:
                    if not in_docstring:
                        # Starting a docstring
                        docstring_delimiter = '"""' if '"""' in line else "'''"
                        in_docstring = True
                        # Check if docstring ends on same line
                        if line.count(docstring_delimiter) >= 2:
                            in_docstring = False
                        continue
                    elif docstring_delimiter in line:
                        # Ending docstring
                        in_docstring = False
                        continue
                
                if not in_docstring:
                    # Remove inline comments
                    if '#' in line:
                        line = line[:line.index('#')]
                    code_lines.append(line)
            
            code_content = '\n'.join(code_lines)
            
            # Should not contain actual SQL queries to public schema in executable code
            forbidden_patterns = [
                r'text\(["\'].*SELECT.*FROM\s+public\.',
                r'text\(["\'].*UPDATE\s+public\.',
                r'text\(["\'].*INSERT\s+INTO\s+public\.',
                r'text\(["\'].*DELETE\s+FROM\s+public\.',
                r'await.*db\.execute.*public\.',
                r'"SELECT.*FROM\s+public\.',  # Direct string with SQL
                r"'SELECT.*FROM\s+public\.",  # Direct string with SQL
                r'f"SELECT.*FROM\s+public\.',  # f-string with SQL
                r"f'SELECT.*FROM\s+public\."   # f-string with SQL
            ]
            
            for pattern in forbidden_patterns:
                matches = re.findall(pattern, code_content, re.IGNORECASE)
                assert len(matches) == 0, f"{file_path} contains forbidden SQL in executable code: {pattern}"
    
    def test_clients_use_http_apis(self):
        """Test that service clients use HTTP APIs instead of direct SQL"""
        client_files = [
            "app/clients/strategy_service_client.py",
            "app/clients/portfolio_service_client.py",
            "app/clients/account_service_client.py",
            "app/clients/analytics_service_client.py"
        ]
        
        for file_path in client_files:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Should contain HTTP client usage
            http_indicators = [
                'httpx',
                'AsyncClient',
                'post(',
                'get(',
                'put(',
                'delete('
            ]
            
            has_http = any(indicator in content for indicator in http_indicators)
            assert has_http, f"{file_path} doesn't appear to use HTTP APIs"
    
    def test_service_discovery_usage(self):
        """Test that clients use service discovery instead of hardcoded URLs"""
        client_files = [
            "app/clients/strategy_service_client.py",
            "app/clients/portfolio_service_client.py",
            "app/clients/account_service_client.py",
            "app/clients/analytics_service_client.py"
        ]
        
        for file_path in client_files:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Should not contain hardcoded localhost URLs
            hardcoded_patterns = [
                r'localhost:8089',
                r'localhost:8013', 
                r'localhost:8011',
                r'localhost:8001',
                r'127\.0\.0\.1:\d+'
            ]
            
            for pattern in hardcoded_patterns:
                matches = re.findall(pattern, content)
                assert len(matches) == 0, f"{file_path} contains hardcoded URL: {pattern}"
            
            # Should use service discovery
            assert '_get_service_port' in content, f"{file_path} doesn't use service discovery"


class TestSecurityImplementation:
    """Test security implementation structure"""
    
    def test_internal_auth_structure(self):
        """Test internal auth file has expected security components"""
        with open("app/security/internal_auth.py", 'r') as f:
            content = f.read()
        
        # Should contain security components
        security_components = [
            'CriticalServiceAuth',
            'AUTHORIZED_SERVICES',
            'validate_service_identity',
            'validate_request_signature',
            'hmac',
            'sha256',
            'X-Service-Identity',
            'X-Internal-API-Key',
            'X-Request-Signature'
        ]
        
        for component in security_components:
            assert component in content, f"Missing security component: {component}"
    
    def test_anomaly_detector_structure(self):
        """Test anomaly detector has expected detection capabilities"""
        with open("app/services/order_anomaly_detector.py", 'r') as f:
            content = f.read()
        
        # Should contain anomaly detection components
        anomaly_components = [
            'OrderAnomalyDetector',
            'AnomalyType',
            'HIGH_FREQUENCY_ORDERS',
            'LARGE_ORDER_SIZE', 
            'OFF_HOURS_ACTIVITY',
            'analyze_order_event',
            'max_orders_per_minute',
            'large_order_quantity'
        ]
        
        for component in anomaly_components:
            assert component in content, f"Missing anomaly detection component: {component}"


class TestRedisMonitoring:
    """Test Redis monitoring implementation"""
    
    def test_redis_monitor_structure(self):
        """Test Redis monitor has expected monitoring capabilities"""
        with open("app/services/redis_usage_monitor.py", 'r') as f:
            content = f.read()
        
        # Should contain monitoring components
        monitor_components = [
            'RedisUsageMonitor',
            'RedisUsagePattern',
            'IDEMPOTENCY',
            'RATE_LIMITING', 
            'CACHING',
            'REAL_TIME_DATA',
            'get_health_status',
            'memory_warning_threshold',
            'memory_critical_threshold'
        ]
        
        for component in monitor_components:
            assert component in content, f"Missing monitoring component: {component}"


class TestDocumentationCompliance:
    """Test architecture documentation exists and is complete"""
    
    def test_architecture_docs_exist(self):
        """Test architecture compliance documentation exists"""
        doc_path = Path("docs/ARCHITECTURE_COMPLIANCE.md")
        assert doc_path.exists(), "Architecture compliance documentation missing"
        
        with open(doc_path, 'r') as f:
            content = f.read()
        
        # Should contain key sections
        required_sections = [
            "Schema Boundary Replacements",
            "Service Discovery Implementation", 
            "Redis Data Plane Monitoring",
            "Enhanced Security Layer",
            "Implementation Guidelines",
            "Testing & Validation"
        ]
        
        for section in required_sections:
            assert section in content, f"Missing documentation section: {section}"
    
    def test_api_endpoint_documentation(self):
        """Test API endpoints are documented"""
        doc_path = Path("docs/ARCHITECTURE_COMPLIANCE.md")
        with open(doc_path, 'r') as f:
            content = f.read()
        
        # Should document required API endpoints
        api_patterns = [
            r'GET\s+/strategies/',
            r'POST\s+/strategies/',
            r'PUT\s+/accounts/',
            r'POST\s+/analytics/',
            r'GET\s+/health/redis'
        ]
        
        for pattern in api_patterns:
            matches = re.findall(pattern, content)
            assert len(matches) > 0, f"Missing API documentation pattern: {pattern}"


class TestConfigurationValidation:
    """Test configuration and environment setup"""
    
    def test_environment_variables_documented(self):
        """Test environment variables are documented in env.test"""
        env_path = Path(".env.test")
        assert env_path.exists(), "Test environment file missing"
        
        with open(env_path, 'r') as f:
            content = f.read()
        
        # Should contain required environment variables
        required_vars = [
            'INTERNAL_API_KEY',
            'JWT_SECRET_KEY',
            'algo_engine_api_key',
            'algo_engine_secret',
            'user_interface_api_key',
            'user_interface_secret'
        ]
        
        for var in required_vars:
            assert var in content, f"Missing environment variable: {var}"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])