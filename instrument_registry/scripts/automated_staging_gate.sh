#!/bin/bash

# =============================================================================
# Automated Staging Gate for Instrument Registry Production Deployment
# =============================================================================
#
# This script automates the final verification checks before production Go-Live.
# It ensures all systems are operational and ready for deployment.
#
# Usage:
#   ./scripts/automated_staging_gate.sh [staging|production]
#
# =============================================================================

set -euo pipefail

ENVIRONMENT="${1:-staging}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$PROJECT_DIR/staging_gate_$(date +%Y%m%d_%H%M%S).log"

# StocksBlitz Configuration
CONFIG_SERVICE_URL="http://localhost:8100"
INTERNAL_API_KEY="AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
INSTRUMENT_REGISTRY_URL="http://localhost:8901"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${timestamp} [${level}] ${message}" | tee -a "$LOG_FILE"
}

info() { log "INFO" "$@"; }
warn() { log "WARN" "${YELLOW}$*${NC}"; }
error() { log "ERROR" "${RED}$*${NC}"; }
success() { log "SUCCESS" "${GREEN}$*${NC}"; }

# Check if service is running
check_service_health() {
    local service_name="$1"
    local url="$2"
    local expected_status="${3:-healthy}"
    
    info "🔍 Checking $service_name health..."
    
    if ! response=$(curl -s --max-time 10 "$url/health" 2>/dev/null); then
        error "❌ $service_name is not responding"
        return 1
    fi
    
    if ! echo "$response" | jq -e ".status == \"$expected_status\"" >/dev/null 2>&1; then
        error "❌ $service_name health check failed: $(echo "$response" | jq -r '.status // "unknown"')"
        return 1
    fi
    
    success "✅ $service_name is healthy"
    return 0
}

# Verify config service integration
verify_config_service() {
    info "🔧 Verifying config service integration..."
    
    local config_keys=(
        "INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL"
        "INSTRUMENT_REGISTRY_PLANNER_TIMEOUT"
        "INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN"
        "INSTRUMENT_REGISTRY_FILTERING_STRICTNESS"
        "INSTRUMENT_REGISTRY_PLAN_CACHE_TTL"
    )
    
    for key in "${config_keys[@]}"; do
        if ! curl -s --max-time 5 \
            -H "X-Internal-API-Key: $INTERNAL_API_KEY" \
            "$CONFIG_SERVICE_URL/api/v1/secrets/$key/value?environment=prod" \
            | jq -e '.secret_value' >/dev/null 2>&1; then
            error "❌ Config parameter $key not accessible"
            return 1
        fi
    done
    
    success "✅ All config parameters accessible"
    return 0
}

# Run test suites
run_test_suite() {
    local test_type="$1"
    local test_file="$2"
    
    info "🧪 Running $test_type tests..."
    
    cd "$PROJECT_DIR"
    
    if ! python3 "$test_file" >/tmp/test_output.log 2>&1; then
        error "❌ $test_type tests failed"
        cat /tmp/test_output.log | tail -20
        return 1
    fi
    
    success "✅ $test_type tests passed"
    return 0
}

# Check evidence artifacts
verify_evidence_artifacts() {
    info "📋 Verifying evidence artifacts..."
    
    local artifacts=(
        "subscription_management_test_report.json"
        "test_search_api_real.py"
        "test_subscription_planner_validation.py"
        "test_planner_unit_validation.py"
    )
    
    for artifact in "${artifacts[@]}"; do
        if [[ ! -f "$PROJECT_DIR/$artifact" ]]; then
            error "❌ Missing evidence artifact: $artifact"
            return 1
        fi
    done
    
    # Check JSON evidence validity
    if [[ -f "$PROJECT_DIR/subscription_management_test_report.json" ]]; then
        if ! jq -e '.summary.total_tests > 0' "$PROJECT_DIR/subscription_management_test_report.json" >/dev/null 2>&1; then
            error "❌ Invalid JSON evidence in subscription_management_test_report.json"
            return 1
        fi
    fi
    
    success "✅ All evidence artifacts verified"
    return 0
}

# Test subscription planning functionality
test_subscription_planning() {
    info "📊 Testing subscription planning functionality..."
    
    # Test plan creation
    local plan_response
    if ! plan_response=$(curl -s --max-time 10 -X POST \
        "$INSTRUMENT_REGISTRY_URL/api/v1/internal/instrument-registry/subscriptions/plan?user_id=staging_test" \
        -H "X-Internal-API-Key: $INTERNAL_API_KEY" \
        -H "Content-Type: application/json" \
        -d '{
            "plan_name": "Staging Verification Plan",
            "subscription_type": "live_feed",
            "instruments": ["NSE:RELIANCE", "NSE:TCS", "BSE:HDFC"]
        }' 2>/dev/null); then
        error "❌ Subscription planning API not responding"
        return 1
    fi
    
    # Verify response structure
    if ! echo "$plan_response" | jq -e '.plan_id' >/dev/null 2>&1; then
        error "❌ Invalid subscription planning response: $plan_response"
        return 1
    fi
    
    local plan_id
    plan_id=$(echo "$plan_response" | jq -r '.plan_id')
    
    # Test plan description
    if ! curl -s --max-time 10 -X POST \
        "$INSTRUMENT_REGISTRY_URL/api/v1/internal/instrument-registry/subscriptions/plan/$plan_id/describe" \
        -H "X-Internal-API-Key: $INTERNAL_API_KEY" \
        -H "Content-Type: application/json" \
        -d '{"description_level": "detailed"}' \
        | jq -e '.plan_id' >/dev/null 2>&1; then
        error "❌ Plan description functionality failed"
        return 1
    fi
    
    success "✅ Subscription planning functionality verified"
    return 0
}

# Check monitoring and metrics
verify_monitoring() {
    info "📈 Verifying monitoring and metrics..."
    
    # Check if metrics endpoint is accessible
    if ! curl -s --max-time 5 "$INSTRUMENT_REGISTRY_URL/metrics" | head -5 >/dev/null 2>&1; then
        warn "⚠️  Metrics endpoint not accessible (may be internal-only)"
    else
        success "✅ Metrics endpoint accessible"
    fi
    
    # Check actuator endpoints
    if ! curl -s --max-time 5 \
        -H "X-Internal-API-Key: $INTERNAL_API_KEY" \
        "$INSTRUMENT_REGISTRY_URL/api/v1/internal/instrument-registry/actuator/health" \
        | jq -e '.status' >/dev/null 2>&1; then
        error "❌ Actuator endpoints not accessible"
        return 1
    fi
    
    success "✅ Monitoring systems verified"
    return 0
}

# Check database connectivity and schema
verify_database() {
    info "🗄️  Verifying database connectivity and schema..."
    
    # Test database connection via health endpoint
    if ! curl -s --max-time 5 "$INSTRUMENT_REGISTRY_URL/health/database" \
        | jq -e '.database_connected == true' >/dev/null 2>&1; then
        error "❌ Database connectivity check failed"
        return 1
    fi
    
    success "✅ Database connectivity verified"
    return 0
}

# Run comprehensive verification
run_comprehensive_verification() {
    info "🚀 Starting comprehensive staging gate verification for $ENVIRONMENT environment"
    info "📝 Log file: $LOG_FILE"
    echo ""
    
    local checks_passed=0
    local total_checks=0
    
    # Array of check functions
    local checks=(
        "check_service_health:Config Service:$CONFIG_SERVICE_URL"
        "verify_config_service"
        "verify_evidence_artifacts"
        "run_test_suite:Unit Tests:test_planner_unit_validation.py"
        "test_subscription_planning"
        "verify_monitoring"
        "verify_database"
    )
    
    for check in "${checks[@]}"; do
        ((total_checks++))
        
        if [[ "$check" == *":"* ]]; then
            IFS=':' read -ra CHECK_PARTS <<< "$check"
            local check_func="${CHECK_PARTS[0]}"
            local check_args=("${CHECK_PARTS[@]:1}")
            
            if "$check_func" "${check_args[@]}"; then
                ((checks_passed++))
            fi
        else
            if "$check"; then
                ((checks_passed++))
            fi
        fi
        
        echo ""
    done
    
    # Final report
    echo "==============================================================================="
    if [[ $checks_passed -eq $total_checks ]]; then
        success "🎉 STAGING GATE PASSED: $checks_passed/$total_checks checks successful"
        success "✅ Instrument Registry is READY FOR PRODUCTION DEPLOYMENT!"
        echo ""
        success "Next steps:"
        success "1. Deploy to production: docker-compose -f docker-compose.production.yml up -d instrument-registry"
        success "2. Monitor dashboards for 30 minutes"
        success "3. Gradually enable features with config toggles"
        echo ""
        return 0
    else
        error "❌ STAGING GATE FAILED: Only $checks_passed/$total_checks checks passed"
        error "🚫 DO NOT DEPLOY TO PRODUCTION"
        error "Please address the failed checks before proceeding"
        echo ""
        return 1
    fi
}

# Main execution
main() {
    echo "==============================================================================="
    echo "           INSTRUMENT REGISTRY STAGING GATE VERIFICATION"
    echo "                       Environment: $ENVIRONMENT"
    echo "==============================================================================="
    echo ""
    
    # Change to project directory
    cd "$PROJECT_DIR"
    
    # Run verification
    if run_comprehensive_verification; then
        exit 0
    else
        exit 1
    fi
}

# Script execution guard
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi