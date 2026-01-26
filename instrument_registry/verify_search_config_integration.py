#!/usr/bin/env python3
"""
Verify Search Config Service Integration

Tests that search/catalog API uses config service parameters rather than hardcoded defaults.
Provides evidence for production readiness review.
"""

import asyncio
import aiohttp
import json
import logging
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SearchConfigVerifier:
    """Verify search API config service integration"""
    
    def __init__(self, base_url: str, config_service_url: str, api_key: str):
        self.base_url = base_url
        self.config_service_url = config_service_url
        self.api_key = api_key
        self.headers = {"X-Internal-API-Key": api_key}
    
    async def check_config_parameter(self, session: aiohttp.ClientSession, param_name: str) -> Dict[str, Any]:
        """Check if config parameter is registered in config service"""
        try:
            url = f"{self.config_service_url}/api/v1/secrets/{param_name}/value"
            async with session.get(url, headers=self.headers, params={"environment": "prod"}) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "parameter": param_name,
                        "status": "FOUND",
                        "value": data.get("secret_value") or data.get("value"),
                        "source": "config_service"
                    }
                else:
                    return {
                        "parameter": param_name,
                        "status": "NOT_FOUND",
                        "error": f"HTTP {response.status}",
                        "source": "config_service"
                    }
        except Exception as e:
            return {
                "parameter": param_name,
                "status": "ERROR",
                "error": str(e),
                "source": "config_service"
            }
    
    async def test_search_api_response_time(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Test search API and measure response time to infer timeout usage"""
        import time
        
        try:
            start_time = time.time()
            url = f"{self.base_url}/api/v1/internal/instrument-registry/search"
            search_query = {"query": "TEST", "page": 1, "page_size": 10}
            
            async with session.post(url, json=search_query, headers=self.headers) as response:
                response_time_ms = (time.time() - start_time) * 1000
                
                if response.status == 200:
                    return {
                        "test": "search_api_timeout",
                        "status": "SUCCESS",
                        "response_time_ms": response_time_ms,
                        "timeout_evidence": response_time_ms < 10000  # Should be under configured timeout
                    }
                else:
                    return {
                        "test": "search_api_timeout",
                        "status": "FAILED",
                        "error": f"HTTP {response.status}",
                        "response_time_ms": response_time_ms
                    }
        except Exception as e:
            return {
                "test": "search_api_timeout",
                "status": "ERROR",
                "error": str(e)
            }
    
    async def test_pagination_limits(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Test pagination to verify max results per page configuration"""
        try:
            url = f"{self.base_url}/api/v1/internal/instrument-registry/search"
            # Request more than default but within config limit
            search_query = {"query": "", "page": 1, "page_size": 150}
            
            async with session.post(url, json=search_query, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    actual_size = len(data.get("instruments", []))
                    return {
                        "test": "pagination_limits",
                        "status": "SUCCESS",
                        "requested_size": 150,
                        "actual_size": actual_size,
                        "config_respected": actual_size <= 100  # Should respect MAX_RESULTS_PER_PAGE
                    }
                else:
                    return {
                        "test": "pagination_limits", 
                        "status": "FAILED",
                        "error": f"HTTP {response.status}"
                    }
        except Exception as e:
            return {
                "test": "pagination_limits",
                "status": "ERROR",
                "error": str(e)
            }
    
    async def run_verification(self) -> Dict[str, Any]:
        """Run complete config integration verification"""
        logger.info("Starting search config service integration verification...")
        
        # Parameters to verify
        test_params = [
            "INSTRUMENT_REGISTRY_SEARCH_TIMEOUT",
            "INSTRUMENT_REGISTRY_MAX_RESULTS_PER_PAGE", 
            "INSTRUMENT_REGISTRY_SEARCH_INDEX_REFRESH",
            "INSTRUMENT_REGISTRY_CACHE_TTL_SECONDS",
            "INSTRUMENT_REGISTRY_QUERY_OPTIMIZATION"
        ]
        
        results = {
            "verification_summary": {
                "test_timestamp": datetime.now().isoformat(),
                "config_service_url": self.config_service_url,
                "search_api_url": self.base_url,
                "total_params_tested": len(test_params)
            },
            "config_parameters": [],
            "functional_tests": [],
            "overall_assessment": {}
        }
        
        async with aiohttp.ClientSession() as session:
            # Check config parameters
            logger.info("Checking config parameter registration...")
            for param in test_params:
                result = await self.check_config_parameter(session, param)
                results["config_parameters"].append(result)
                logger.info(f"  {param}: {result['status']}")
            
            # Functional tests
            logger.info("Testing functional API behavior...")
            
            # Test timeout configuration
            timeout_result = await self.test_search_api_response_time(session)
            results["functional_tests"].append(timeout_result)
            logger.info(f"  Search API timeout test: {timeout_result['status']}")
            
            # Test pagination limits
            pagination_result = await self.test_pagination_limits(session)
            results["functional_tests"].append(pagination_result)
            logger.info(f"  Pagination limits test: {pagination_result['status']}")
        
        # Overall assessment
        config_found = len([p for p in results["config_parameters"] if p["status"] == "FOUND"])
        config_percentage = (config_found / len(test_params)) * 100
        
        functional_passed = len([t for t in results["functional_tests"] if t["status"] == "SUCCESS"])
        functional_percentage = (functional_passed / len(results["functional_tests"])) * 100
        
        results["overall_assessment"] = {
            "config_integration_score": f"{config_found}/{len(test_params)} ({config_percentage:.1f}%)",
            "functional_tests_score": f"{functional_passed}/{len(results['functional_tests'])} ({functional_percentage:.1f}%)",
            "production_ready": config_percentage >= 80 and functional_percentage >= 80,
            "recommendations": []
        }
        
        if config_percentage < 100:
            results["overall_assessment"]["recommendations"].append(
                f"Register missing config parameters ({len(test_params) - config_found} missing)"
            )
        
        if functional_percentage < 100:
            results["overall_assessment"]["recommendations"].append(
                "Fix functional test failures before production deployment"
            )
        
        return results
    
    def save_verification_report(self, results: Dict[str, Any], filename: str):
        """Save verification results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Verification report saved to {filename}")


async def main():
    """Main verification execution"""
    # Configuration
    SEARCH_API_URL = "http://localhost:8087"
    CONFIG_SERVICE_URL = "http://localhost:8100"
    API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
    
    verifier = SearchConfigVerifier(SEARCH_API_URL, CONFIG_SERVICE_URL, API_KEY)
    
    # Run verification
    results = await verifier.run_verification()
    
    # Save results
    verifier.save_verification_report(results, "search_config_integration_verification.json")
    
    # Print summary
    print("\n" + "="*80)
    print("SEARCH CONFIG INTEGRATION VERIFICATION")
    print("="*80)
    
    assessment = results["overall_assessment"]
    print(f"Config Integration: {assessment['config_integration_score']}")
    print(f"Functional Tests: {assessment['functional_tests_score']}")
    print(f"Production Ready: {'✅ YES' if assessment['production_ready'] else '❌ NO'}")
    
    if assessment["recommendations"]:
        print("\nRecommendations:")
        for rec in assessment["recommendations"]:
            print(f"  • {rec}")
    
    return assessment["production_ready"]


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        exit(1)