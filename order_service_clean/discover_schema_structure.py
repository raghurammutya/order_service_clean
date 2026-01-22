#!/usr/bin/env python3
"""
Database Schema Discovery Script

Analyzes the actual database structure to understand:
1. What schemas exist
2. What tables are in each schema  
3. What tables the code expects vs. what actually exists
4. Which public.* references are invalid

This helps determine the correct migration path.
"""

import asyncio
import os
from typing import Dict, List, Set
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError


class SchemaMismatchAnalyzer:
    """Analyzes schema mismatches between code expectations and database reality"""
    
    def __init__(self, database_url: str = None):
        """Initialize with database connection"""
        self.database_url = database_url or self._get_database_url()
        self.engine = None
        self.schemas = {}
        self.code_references = set()
        
    def _get_database_url(self) -> str:
        """Get database URL from environment or default"""
        return os.getenv(
            'DATABASE_URL', 
            'postgresql://postgres:password@localhost:5432/order_service_db'
        )
    
    def connect(self):
        """Create database connection"""
        try:
            self.engine = create_engine(self.database_url)
            print(f"✅ Connected to database: {self.database_url}")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            print("💡 Make sure DATABASE_URL is set or database is running")
            return False
        return True
    
    def discover_schemas_and_tables(self) -> Dict[str, List[str]]:
        """Discover all schemas and their tables"""
        if not self.engine:
            return {}
            
        try:
            inspector = inspect(self.engine)
            schemas = {}
            
            # Get all schemas
            schema_names = inspector.get_schema_names()
            print(f"\n📋 Found {len(schema_names)} schemas:")
            
            for schema in schema_names:
                try:
                    tables = inspector.get_table_names(schema=schema)
                    schemas[schema] = tables
                    print(f"  📁 {schema}: {len(tables)} tables")
                    for table in tables[:5]:  # Show first 5 tables
                        print(f"    └─ {table}")
                    if len(tables) > 5:
                        print(f"    └─ ... and {len(tables) - 5} more")
                except Exception as e:
                    print(f"  ❌ Error reading schema {schema}: {e}")
                    schemas[schema] = []
            
            self.schemas = schemas
            return schemas
            
        except Exception as e:
            print(f"❌ Error discovering schemas: {e}")
            return {}
    
    def find_code_references(self) -> Set[str]:
        """Find all public.* table references in the code"""
        import re
        import glob
        
        public_refs = set()
        
        # Search Python files
        python_files = glob.glob("app/**/*.py", recursive=True)
        sql_files = glob.glob("migrations/*.sql", recursive=True)
        
        pattern = r'public\.(\w+)'
        
        for file_path in python_files + sql_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        public_refs.add(f"public.{match}")
            except Exception as e:
                print(f"⚠️  Error reading {file_path}: {e}")
        
        self.code_references = public_refs
        return public_refs
    
    def check_table_existence(self) -> Dict[str, Dict]:
        """Check which tables referenced in code actually exist"""
        results = {}
        
        for ref in self.code_references:
            schema_name, table_name = ref.split('.', 1)
            
            exists = False
            actual_schema = None
            
            # Check if table exists in the referenced schema
            if schema_name in self.schemas and table_name in self.schemas[schema_name]:
                exists = True
                actual_schema = schema_name
            else:
                # Check if table exists in other schemas
                for schema, tables in self.schemas.items():
                    if table_name in tables:
                        exists = True
                        actual_schema = schema
                        break
            
            results[ref] = {
                'exists': exists,
                'actual_schema': actual_schema,
                'expected_schema': schema_name,
                'table_name': table_name,
                'schema_mismatch': actual_schema != schema_name if exists else None
            }
        
        return results
    
    def analyze_mismatches(self):
        """Perform complete analysis and report findings"""
        print("\n" + "="*60)
        print("🔍 DATABASE SCHEMA ANALYSIS")
        print("="*60)
        
        if not self.connect():
            return
            
        # Step 1: Discover actual database structure
        print("\n1️⃣ DISCOVERING DATABASE SCHEMAS...")
        schemas = self.discover_schemas_and_tables()
        
        # Step 2: Find code references
        print("\n2️⃣ SCANNING CODE FOR public.* REFERENCES...")
        code_refs = self.find_code_references()
        print(f"Found {len(code_refs)} public.* references in code:")
        for ref in sorted(code_refs):
            print(f"  📍 {ref}")
        
        # Step 3: Check existence
        print("\n3️⃣ CHECKING TABLE EXISTENCE...")
        existence_check = self.check_table_existence()
        
        # Report findings
        print("\n📊 ANALYSIS RESULTS:")
        print("-" * 40)
        
        missing_tables = []
        schema_mismatches = []
        valid_references = []
        
        for ref, info in existence_check.items():
            if not info['exists']:
                missing_tables.append(ref)
                print(f"❌ MISSING: {ref}")
            elif info['schema_mismatch']:
                schema_mismatches.append((ref, info['actual_schema']))
                print(f"📦 SCHEMA MISMATCH: {ref} → found in {info['actual_schema']}")
            else:
                valid_references.append(ref)
                print(f"✅ VALID: {ref}")
        
        # Summary and recommendations
        print("\n" + "="*60)
        print("📋 SUMMARY & RECOMMENDATIONS")
        print("="*60)
        
        if missing_tables:
            print(f"\n❌ MISSING TABLES ({len(missing_tables)}):")
            for table in missing_tables:
                print(f"  • {table}")
            print("\n💡 These tables don't exist - code should use API calls instead")
        
        if schema_mismatches:
            print(f"\n📦 SCHEMA MISMATCHES ({len(schema_mismatches)}):")
            for ref, actual_schema in schema_mismatches:
                print(f"  • {ref} → use {actual_schema}.{ref.split('.')[1]} instead")
            print("\n💡 Update code to reference correct schema")
        
        if valid_references:
            print(f"\n✅ VALID REFERENCES ({len(valid_references)}):")
            for ref in valid_references:
                print(f"  • {ref}")
        
        # Architecture recommendations
        print("\n🏗️ ARCHITECTURE RECOMMENDATIONS:")
        if missing_tables or schema_mismatches:
            print("  1. Replace missing table access with service API calls")
            print("  2. Update schema references to match actual database structure")
            print("  3. Remove foreign key constraints to non-existent tables")
            print("  4. Implement API-based validation instead of DB constraints")
        else:
            print("  ✅ Schema references are consistent")
            
        print("\n" + "="*60)


def main():
    """Main analysis function"""
    analyzer = SchemaMismatchAnalyzer()
    analyzer.analyze_mismatches()


if __name__ == "__main__":
    main()