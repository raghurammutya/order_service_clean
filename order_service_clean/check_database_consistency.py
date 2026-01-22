#!/usr/bin/env python3
"""
Database Consistency Checker

Performs two critical consistency checks:
1. All tables referenced in codebase actually exist in order_service schema
2. All tables in order_service schema are actually used in codebase

This ensures:
- No runtime errors from missing tables
- No orphaned tables taking up space
- Clean database schema design
"""

import os
import re
import glob
from typing import Dict, List, Set, Tuple
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError


class DatabaseConsistencyChecker:
    """Check consistency between code references and actual database schema"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or self._get_database_url()
        self.engine = None
        self.order_service_tables = set()
        self.code_table_refs = set()
        
    def _get_database_url(self) -> str:
        """Get database URL from environment"""
        return os.getenv(
            'DATABASE_URL', 
            'postgresql://postgres:password@localhost:5432/order_service_db'
        )
    
    def connect(self) -> bool:
        """Create database connection"""
        try:
            self.engine = create_engine(self.database_url)
            return True
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            return False
    
    def get_order_service_tables(self) -> Set[str]:
        """Get all tables that actually exist in order_service schema"""
        if not self.engine:
            return set()
            
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema='order_service')
            self.order_service_tables = set(tables)
            return self.order_service_tables
            
        except Exception as e:
            print(f"❌ Error reading order_service schema: {e}")
            return set()
    
    def find_code_table_references(self) -> Set[str]:
        """Find all order_service.* table references in code"""
        table_refs = set()
        
        # Search Python files and SQL migrations
        file_patterns = ["app/**/*.py", "migrations/*.sql"]
        
        # Patterns to find table references
        patterns = [
            r'order_service\.(\w+)',           # order_service.table_name
            r'FROM\s+(\w+)',                   # FROM table_name (unqualified)
            r'INSERT\s+INTO\s+(\w+)',         # INSERT INTO table_name 
            r'UPDATE\s+(\w+)',                # UPDATE table_name
            r'DELETE\s+FROM\s+(\w+)',         # DELETE FROM table_name
            r'JOIN\s+(\w+)',                  # JOIN table_name
            r'CREATE\s+TABLE.*?(\w+)',        # CREATE TABLE table_name
            r'ALTER\s+TABLE\s+(\w+)',         # ALTER TABLE table_name
            r'__tablename__\s*=\s*["\'](\w+)["\']',  # SQLAlchemy __tablename__
        ]
        
        for file_pattern in file_patterns:
            files = glob.glob(file_pattern, recursive=True)
            
            for file_path in files:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # Find order_service schema references
                    for pattern in patterns:
                        matches = re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE)
                        for match in matches:
                            table_name = match.group(1).lower()
                            
                            # Filter out common non-table names
                            if not self._is_likely_table_name(table_name):
                                continue
                                
                            table_refs.add(table_name)
                            
                except Exception as e:
                    continue  # Skip files with encoding issues
        
        # Remove common false positives
        false_positives = {
            'information_schema', 'pg_', 'now', 'current_date', 'current_time',
            'coalesce', 'count', 'sum', 'max', 'min', 'avg', 'distinct',
            'select', 'where', 'order', 'by', 'group', 'having', 'limit',
            'text', 'integer', 'bigint', 'varchar', 'boolean', 'timestamp'
        }
        
        filtered_refs = {ref for ref in table_refs 
                        if not any(fp in ref.lower() for fp in false_positives)
                        and len(ref) > 2}
        
        self.code_table_refs = filtered_refs
        return filtered_refs
    
    def _is_likely_table_name(self, name: str) -> bool:
        """Check if a name is likely to be a table name"""
        name = name.lower()
        
        # Skip SQL keywords and functions
        sql_keywords = {
            'select', 'from', 'where', 'join', 'inner', 'left', 'right', 'outer',
            'on', 'group', 'order', 'by', 'having', 'limit', 'offset',
            'insert', 'into', 'values', 'update', 'set', 'delete',
            'create', 'table', 'alter', 'drop', 'index', 'constraint',
            'primary', 'foreign', 'key', 'references', 'not', 'null',
            'default', 'auto_increment', 'unique', 'check',
            'and', 'or', 'in', 'like', 'between', 'is', 'exists',
            'case', 'when', 'then', 'else', 'end',
            'count', 'sum', 'avg', 'min', 'max', 'distinct',
            'as', 'alias', 'with', 'union', 'intersect', 'except'
        }
        
        if name in sql_keywords:
            return False
            
        # Must be reasonable table name (letters, numbers, underscores)
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', name):
            return False
            
        return True
    
    def analyze_consistency(self):
        """Perform comprehensive database consistency analysis"""
        print("\n" + "="*80)
        print("🔍 DATABASE CONSISTENCY ANALYSIS")
        print("="*80)
        print("Checking alignment between code references and actual database schema")
        
        if not self.connect():
            print("❌ Cannot connect to database - skipping analysis")
            return
            
        print("\n1️⃣ DISCOVERING order_service SCHEMA TABLES...")
        actual_tables = self.get_order_service_tables()
        print(f"Found {len(actual_tables)} tables in order_service schema:")
        for table in sorted(actual_tables):
            print(f"  📁 order_service.{table}")
        
        print("\n2️⃣ SCANNING CODE FOR TABLE REFERENCES...")
        referenced_tables = self.find_code_table_references()
        print(f"Found {len(referenced_tables)} table references in code:")
        for table in sorted(referenced_tables):
            print(f"  📋 {table}")
        
        print("\n" + "="*80)
        print("📊 CONSISTENCY ANALYSIS RESULTS")
        print("="*80)
        
        # Check 1: Referenced tables that don't exist
        print(f"\n❌ MISSING TABLES (referenced in code but don't exist):")
        missing_tables = referenced_tables - actual_tables
        if missing_tables:
            print(f"   Found {len(missing_tables)} missing tables:")
            for table in sorted(missing_tables):
                print(f"   🚫 {table} - referenced in code but doesn't exist")
                self._show_table_usage(table)
        else:
            print(f"   ✅ All referenced tables exist in database")
        
        # Check 2: Existing tables that aren't referenced
        print(f"\n🗂️  UNUSED TABLES (exist in database but not referenced in code):")
        unused_tables = actual_tables - referenced_tables
        if unused_tables:
            print(f"   Found {len(unused_tables)} unused tables:")
            for table in sorted(unused_tables):
                print(f"   📦 order_service.{table} - exists but not used in code")
        else:
            print(f"   ✅ All database tables are referenced in code")
        
        # Summary and recommendations
        print(f"\n" + "="*80)
        print("📋 SUMMARY & RECOMMENDATIONS")
        print("="*80)
        
        if missing_tables:
            print(f"\n🚨 CRITICAL ISSUES ({len(missing_tables)} missing tables):")
            print("   These will cause runtime errors when the code tries to access them:")
            for table in sorted(missing_tables):
                print(f"   • {table}")
            print("\n💡 FIXES NEEDED:")
            print("   1. Create missing tables with proper migrations")
            print("   2. OR replace code references with API calls")
            print("   3. OR remove dead code that references non-existent tables")
        
        if unused_tables:
            print(f"\n🧹 CLEANUP OPPORTUNITIES ({len(unused_tables)} unused tables):")
            print("   These tables exist but aren't used - consider cleanup:")
            for table in sorted(unused_tables):
                print(f"   • order_service.{table}")
            print("\n💡 OPTIONS:")
            print("   1. Add code to use these tables if they serve a purpose")
            print("   2. OR remove unused tables to clean up schema")
            print("   3. OR document why tables exist without code references")
        
        if not missing_tables and not unused_tables:
            print("\n🎉 ✅ PERFECT CONSISTENCY!")
            print("   All table references align perfectly with database schema")
        
        print(f"\n📊 STATISTICS:")
        print(f"   • Tables in database: {len(actual_tables)}")
        print(f"   • Tables referenced in code: {len(referenced_tables)}")
        print(f"   • Missing tables: {len(missing_tables)}")
        print(f"   • Unused tables: {len(unused_tables)}")
        print(f"   • Consistency score: {((len(actual_tables) + len(referenced_tables) - len(missing_tables) - len(unused_tables)) / max(len(actual_tables) + len(referenced_tables), 1) * 100):.1f}%")
        
        print("\n" + "="*80)
    
    def _show_table_usage(self, table_name: str):
        """Show where a table is used in the codebase"""
        file_patterns = ["app/**/*.py", "migrations/*.sql"]
        usage_count = 0
        
        for file_pattern in file_patterns:
            files = glob.glob(file_pattern, recursive=True)
            
            for file_path in files[:5]:  # Limit to first 5 files
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    if table_name.lower() in content.lower():
                        usage_count += 1
                        if usage_count <= 3:  # Show first 3 usages
                            print(f"      📍 Used in: {file_path}")
                            
                except Exception:
                    continue
        
        if usage_count > 3:
            print(f"      📍 ... and {usage_count - 3} more files")


def main():
    """Run database consistency check"""
    checker = DatabaseConsistencyChecker()
    checker.analyze_consistency()


if __name__ == "__main__":
    main()