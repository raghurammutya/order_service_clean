#!/usr/bin/env python3
"""
Production Test Runner for Order Service
Generates evidence for production signoff
"""
import subprocess
import sys
import json
import time
from datetime import datetime
from pathlib import Path


def run_command(cmd: list, capture_output: bool = True) -> dict:
    """Run a command and return results"""
    start_time = time.time()
    result = subprocess.run(
        cmd, 
        capture_output=capture_output, 
        text=True,
        cwd=Path(__file__).parent
    )
    end_time = time.time()
    
    return {
        "command": " ".join(cmd),
        "returncode": result.returncode,
        "duration_seconds": round(end_time - start_time, 2),
        "stdout": result.stdout if capture_output else "",
        "stderr": result.stderr if capture_output else ""
    }


def main():
    """Run comprehensive test suite and generate evidence"""
    print("🚀 Order Service Production Test Suite")
    print("=" * 50)
    
    evidence = {
        "test_run_timestamp": datetime.now().isoformat(),
        "environment": "production_validation",
        "test_results": {}
    }
    
    # 1. Syntax and Import Validation
    print("\n📋 1. Syntax and Import Validation")
    result = run_command([sys.executable, "-m", "py_compile", "app/main.py"])
    evidence["test_results"]["syntax_check"] = result
    print(f"   ✅ Syntax check: {'PASS' if result['returncode'] == 0 else 'FAIL'}")
    
    # 2. Security Tests
    print("\n🔒 2. Security Tests")
    result = run_command([
        sys.executable, "-m", "pytest", 
        "tests/test_authentication_security.py", 
        "-v", "--tb=short"
    ])
    evidence["test_results"]["security_tests"] = result
    print(f"   ✅ Security tests: {'PASS' if result['returncode'] == 0 else 'FAIL'}")
    
    # 3. Configuration Tests
    print("\n⚙️  3. Configuration Externalization Tests")
    result = run_command([
        sys.executable, "-m", "pytest",
        "tests/test_configuration_externalization.py",
        "-v", "--tb=short"
    ])
    evidence["test_results"]["configuration_tests"] = result
    print(f"   ✅ Configuration tests: {'PASS' if result['returncode'] == 0 else 'FAIL'}")
    
    # 4. Broker Integration Tests
    print("\n🔗 4. Broker Integration Tests")
    result = run_command([
        sys.executable, "-m", "pytest",
        "tests/test_broker_integration.py",
        "-v", "--tb=short"
    ])
    evidence["test_results"]["broker_integration_tests"] = result
    print(f"   ✅ Broker integration: {'PASS' if result['returncode'] == 0 else 'FAIL'}")
    
    # 5. Business Logic Tests
    print("\n💼 5. Business Logic Implementation Tests")
    result = run_command([
        sys.executable, "-m", "pytest",
        "tests/test_business_logic_implementation.py", 
        "-v", "--tb=short"
    ])
    evidence["test_results"]["business_logic_tests"] = result
    print(f"   ✅ Business logic: {'PASS' if result['returncode'] == 0 else 'FAIL'}")
    
    # 6. Existing Test Suite
    print("\n🧪 6. Existing Test Suite")
    result = run_command([
        sys.executable, "-m", "pytest",
        "tests/", "-x", "--tb=short",
        "--ignore=tests/test_authentication_security.py",
        "--ignore=tests/test_configuration_externalization.py", 
        "--ignore=tests/test_broker_integration.py",
        "--ignore=tests/test_business_logic_implementation.py"
    ])
    evidence["test_results"]["existing_tests"] = result
    print(f"   ✅ Existing tests: {'PASS' if result['returncode'] == 0 else 'FAIL'}")
    
    # 7. Code Quality Checks
    print("\n🔍 7. Code Quality Checks")
    
    # Check for TODO/FIXME
    result = run_command([
        "grep", "-r", "-n", 
        "--include=*.py",
        "-E", "(TODO|FIXME|XXX|HACK)",
        "app/"
    ])
    todo_count = len(result["stdout"].split('\n')) if result["stdout"] else 0
    evidence["test_results"]["todo_check"] = {
        **result,
        "todo_count": todo_count
    }
    print(f"   ⚠️  TODO/FIXME found: {todo_count}")
    
    # Check for pass statements
    result = run_command([
        "grep", "-r", "-n",
        "--include=*.py", 
        "^[[:space:]]*pass[[:space:]]*$",
        "app/"
    ])
    pass_count = len(result["stdout"].split('\n')) if result["stdout"] else 0
    evidence["test_results"]["pass_statement_check"] = {
        **result,
        "pass_count": pass_count
    }
    print(f"   ⚠️  Pass statements found: {pass_count}")
    
    # 8. Generate Summary
    print("\n📊 Test Results Summary")
    print("=" * 30)
    
    all_tests_passed = True
    for test_name, test_result in evidence["test_results"].items():
        if isinstance(test_result, dict) and "returncode" in test_result:
            passed = test_result["returncode"] == 0
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"   {test_name:.<30} {status}")
            if not passed:
                all_tests_passed = False
    
    print(f"\n🎯 Overall Result: {'✅ PRODUCTION READY' if all_tests_passed else '❌ NEEDS FIXES'}")
    
    # Save evidence
    evidence_file = Path("production_test_evidence.json")
    with open(evidence_file, "w") as f:
        json.dump(evidence, f, indent=2, default=str)
    
    print(f"\n📝 Evidence saved to: {evidence_file}")
    print("\n🚀 Production validation complete!")
    
    return 0 if all_tests_passed else 1


if __name__ == "__main__":
    sys.exit(main())