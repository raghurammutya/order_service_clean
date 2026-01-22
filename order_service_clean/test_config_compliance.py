#!/usr/bin/env python3
"""
Config-Service Compliance Test

Tests that the order service properly uses config-service and fails fast
when config service is unavailable in production mode.
"""
import os
import sys

def test_production_fail_fast():
    """Test that service fails fast in production when config service unavailable"""
    print("🔧 Testing production fail-fast behavior...")
    
    # Set production environment
    os.environ['ENVIRONMENT'] = 'production'
    os.environ['CONFIG_SERVICE_URL'] = 'http://localhost:8100'
    os.environ['INTERNAL_API_KEY'] = 'test-key'
    
    try:
        # Clear any test mode flags
        if 'TEST_MODE' in os.environ:
            del os.environ['TEST_MODE']
        
        # Try to import settings in production mode
        from app.config.settings import Settings
        settings = Settings()
        
        print("❌ FAIL: Service should have failed fast without config service in production")
        return False
        
    except SystemExit as e:
        print("✅ PASS: Service correctly failed fast in production mode")
        return True
    except Exception as e:
        print(f"✅ PASS: Service correctly failed with: {e}")
        return True


def test_test_mode_fallback():
    """Test that service works with test defaults when config service unavailable"""
    print("\n🧪 Testing test mode fallback behavior...")
    
    # Set test environment
    os.environ['ENVIRONMENT'] = 'test'
    os.environ['CONFIG_SERVICE_URL'] = 'http://localhost:8100'
    os.environ['INTERNAL_API_KEY'] = 'test-key'
    os.environ['TEST_MODE'] = 'true'
    
    try:
        from app.config.settings import Settings
        settings = Settings()
        
        print("✅ PASS: Settings loaded successfully in test mode")
        print(f"  Environment: {settings.environment}")
        print(f"  Database URL: {settings.database_url}")
        print(f"  Port: {settings.port}")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: Test mode should work with defaults: {e}")
        return False


def test_bootstrap_triad_only():
    """Test that only bootstrap triad environment variables are used"""
    print("\n🔍 Testing bootstrap triad compliance...")
    
    os.environ['ENVIRONMENT'] = 'test'
    os.environ['CONFIG_SERVICE_URL'] = 'http://localhost:8100'
    os.environ['INTERNAL_API_KEY'] = 'test-key'
    os.environ['TEST_MODE'] = 'true'
    
    # Add some environment variables that should NOT be used
    os.environ['DATABASE_URL'] = 'postgresql://should-not-be-used'
    os.environ['PORT'] = '9999'
    os.environ['REDIS_URL'] = 'redis://should-not-be-used'
    
    try:
        from app.config.settings import Settings
        settings = Settings()
        
        # Verify that env vars are NOT used (should use test defaults)
        if 'should-not-be-used' in settings.database_url:
            print("❌ FAIL: Service used DATABASE_URL environment variable instead of config service")
            return False
            
        if settings.port == 9999:
            print("❌ FAIL: Service used PORT environment variable instead of config service")
            return False
            
        print("✅ PASS: Service ignored non-bootstrap environment variables")
        print(f"  Database URL: {settings.database_url} (test default, not env var)")
        print(f"  Port: {settings.port} (from config service, not env var)")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: Bootstrap triad test failed: {e}")
        return False


def main():
    """Run all compliance tests"""
    print("🚀 Config-Service Compliance Test Suite")
    print("=" * 50)
    
    tests = [
        test_test_mode_fallback,      # Run test mode first to avoid import issues
        test_bootstrap_triad_only,
        test_production_fail_fast,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        # Clear imported modules to ensure fresh import
        modules_to_clear = [m for m in sys.modules.keys() if m.startswith('app.')]
        for module in modules_to_clear:
            if module in sys.modules:
                del sys.modules[module]
        
        if test():
            passed += 1
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED - Config service compliance verified!")
        return True
    else:
        print("❌ SOME TESTS FAILED - Config service compliance issues detected")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)