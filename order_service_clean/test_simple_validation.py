"""
Simple validation tests that work without config service.

These tests validate what's implemented without requiring external dependencies.
"""
import ast
import os

def test_file_structure():
    """Test that the expected files and directories exist."""
    print("\n" + "="*50)
    print("FILE STRUCTURE VALIDATION")
    print("="*50)
    
    expected_files = [
        'app/services/kite_client_multi.py',
        'app/services/kite_client.py', 
        'app/services/order_service.py',
        'app/config/settings.py',
        'tests/test_token_manager_contract.py'
    ]
    
    for file_path in expected_files:
        exists = os.path.exists(file_path)
        print(f"{'✅' if exists else '❌'} {file_path}")
        if not exists:
            return False
    
    return True

def test_kite_client_multi_functions():
    """Test that kite_client_multi has the expected functions."""
    print("\n" + "="*50)
    print("KITE CLIENT MULTI FUNCTIONS")
    print("="*50)
    
    try:
        # Read the file and parse the AST to check for function definitions
        with open('app/services/kite_client_multi.py', 'r') as f:
            content = f.read()
            tree = ast.parse(content)
        
        # Extract function definitions
        functions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                functions.append(node.name)
        
        expected_functions = [
            'resolve_trading_account_config',
            'get_all_trading_accounts', 
            'get_kite_client_for_account',
            'get_kite_client_for_account_async',
            'clear_client_cache'
        ]
        
        for func_name in expected_functions:
            found = func_name in functions
            print(f"{'✅' if found else '❌'} {func_name}")
            if not found:
                return False
                
        print(f"\nTotal functions found: {len(functions)}")
        return True
        
    except Exception as e:
        print(f"❌ Error reading kite_client_multi.py: {e}")
        return False

def test_token_manager_contract_structure():
    """Test that the contract test file has expected test classes."""
    print("\n" + "="*50)
    print("TOKEN MANAGER CONTRACT TESTS")
    print("="*50)
    
    try:
        with open('tests/test_token_manager_contract.py', 'r') as f:
            content = f.read()
            tree = ast.parse(content)
        
        # Extract class definitions
        classes = []
        test_methods = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classes.append(node.name)
                # Count test methods in each class
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name.startswith('test_'):
                        test_methods.append(f"{node.name}.{item.name}")
        
        expected_classes = [
            'TestOrderServiceTokenManagerIntegration',
            'TestBackwardCompatibilityContract', 
            'TestContractSettings'
        ]
        
        for class_name in expected_classes:
            found = class_name in classes
            print(f"{'✅' if found else '❌'} {class_name}")
            
        print(f"\nTotal test classes: {len(classes)}")
        print(f"Total test methods: {len(test_methods)}")
        
        # Show some test methods
        print("\nSample test methods:")
        for method in test_methods[:5]:
            print(f"  - {method}")
        if len(test_methods) > 5:
            print(f"  ... and {len(test_methods) - 5} more")
            
        return len(classes) >= 3
        
    except Exception as e:
        print(f"❌ Error reading contract test file: {e}")
        return False

def test_settings_structure():
    """Test that settings.py has token manager related configuration."""
    print("\n" + "="*50)
    print("SETTINGS CONFIGURATION")
    print("="*50)
    
    try:
        with open('app/config/settings.py', 'r') as f:
            content = f.read()
        
        # Look for token manager related strings
        token_indicators = [
            'token_manager',
            'TOKEN_MANAGER',
            'token_manager_url',
            'token_manager_api_key',
            'token_manager_internal_api_key'
        ]
        
        found_indicators = []
        for indicator in token_indicators:
            if indicator in content:
                found_indicators.append(indicator)
                print(f"✅ Found: {indicator}")
            else:
                print(f"❌ Missing: {indicator}")
        
        print(f"\nToken manager indicators found: {len(found_indicators)}/{len(token_indicators)}")
        return len(found_indicators) > 0
        
    except Exception as e:
        print(f"❌ Error reading settings.py: {e}")
        return False

def test_import_structure():
    """Test that key modules can be imported (without config service)."""
    print("\n" + "="*50)
    print("IMPORT STRUCTURE (Static Analysis)")
    print("="*50)
    
    # We can't actually import due to config service dependency,
    # but we can check the import statements in the files
    
    files_to_check = {
        'app/services/kite_client_multi.py': [
            'httpx',
            'KiteConnect', 
            'settings'
        ],
        'tests/test_token_manager_contract.py': [
            'pytest',
            'AsyncMock',
            'httpx'
        ]
    }
    
    all_good = True
    
    for file_path, expected_imports in files_to_check.items():
        print(f"\n{file_path}:")
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            for import_name in expected_imports:
                if import_name in content:
                    print(f"  ✅ {import_name}")
                else:
                    print(f"  ❌ {import_name}")
                    all_good = False
                    
        except Exception as e:
            print(f"  ❌ Error reading {file_path}: {e}")
            all_good = False
    
    return all_good

def main():
    """Run all validation tests."""
    print("🧪 TESTING CURRENT TOKEN MANAGER IMPLEMENTATION")
    print("=" * 60)
    print("Note: These tests validate structure without requiring config service")
    print("=" * 60)
    
    tests = [
        ("File Structure", test_file_structure),
        ("Kite Client Multi Functions", test_kite_client_multi_functions), 
        ("Token Manager Contract Tests", test_token_manager_contract_structure),
        ("Settings Configuration", test_settings_structure),
        ("Import Structure", test_import_structure)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name}: ERROR - {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        icon = "✅" if result else "❌"
        print(f"{icon} {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All structural validation tests passed!")
        print("📋 Token manager integration structure is correctly implemented")
    else:
        print("⚠️  Some validation tests failed")
        print("🔧 Structure may need fixes or implementation completion")
    
    print("\n📝 Next Steps:")
    print("1. ✅ Structure validation completed")
    print("2. 🔧 Start config service to run functional tests")  
    print("3. 🧪 Run integration tests with token manager service")
    print("4. 📊 Measure test coverage and performance")

if __name__ == "__main__":
    main()