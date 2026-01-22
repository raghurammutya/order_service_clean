#!/usr/bin/env python3
"""
Schema Compliance Checker

Verifies that order_service code ONLY accesses order_service.* tables
and uses APIs for all other schemas.

RULE: order_service can ONLY access:
✅ order_service.orders
✅ order_service.trades  
✅ order_service.positions
✅ order_service.* (any table in order_service schema)

❌ NO access to: public.*, user_service.*, backend.*, etc.
"""

import re
import glob
from typing import Dict, List, Set, Tuple


class SchemaComplianceChecker:
    """Check that order_service only accesses its own schema"""
    
    def __init__(self):
        self.violations = []
        self.valid_schemas = {'order_service'}  # Only allowed schema
        self.api_replacements = {
            'strategy': 'Strategy Service API',
            'portfolio': 'Portfolio Service API', 
            'kite_accounts': 'Account Service API',
            'instrument_registry': 'Market Data Service API',
            'strategy_pnl_metrics': 'Analytics Service API',
            'holdings': 'User Service API'
        }
    
    def scan_sql_queries(self) -> List[Dict]:
        """Find all SQL queries that access non-order_service schemas"""
        violations = []
        
        # Patterns to find schema.table references
        schema_table_pattern = r'(\w+)\.(\w+)'
        sql_patterns = [
            r'SELECT.*?FROM\s+(\w+\.\w+)',
            r'INSERT\s+INTO\s+(\w+\.\w+)',
            r'UPDATE\s+(\w+\.\w+)',
            r'DELETE\s+FROM\s+(\w+\.\w+)',
            r'JOIN\s+(\w+\.\w+)',
            r'LEFT\s+JOIN\s+(\w+\.\w+)',
            r'RIGHT\s+JOIN\s+(\w+\.\w+)',
            r'INNER\s+JOIN\s+(\w+\.\w+)',
        ]
        
        # Search Python and SQL files
        file_patterns = ["app/**/*.py", "migrations/*.sql"]
        
        for pattern in file_patterns:
            files = glob.glob(pattern, recursive=True)
            
            for file_path in files:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        
                    # Find SQL with schema references
                    for sql_pattern in sql_patterns:
                        matches = re.finditer(sql_pattern, content, re.IGNORECASE | re.MULTILINE)
                        for match in matches:
                            schema_table = match.group(1)
                            
                            if '.' in schema_table:
                                schema, table = schema_table.split('.', 1)
                                
                                # Check if accessing non-order_service schema
                                if schema.lower() not in self.valid_schemas:
                                    violations.append({
                                        'file': file_path,
                                        'line': self._get_line_number(content, match.start()),
                                        'schema': schema,
                                        'table': table,
                                        'full_ref': schema_table,
                                        'sql_type': sql_pattern.split('\\s+')[0].replace('\\', ''),
                                        'context': self._get_context(content, match.start())
                                    })
                                    
                except Exception as e:
                    print(f"⚠️  Error scanning {file_path}: {e}")
        
        return violations
    
    def _get_line_number(self, content: str, position: int) -> int:
        """Get line number for a position in content"""
        return content[:position].count('\n') + 1
    
    def _get_context(self, content: str, position: int) -> str:
        """Get context around the match"""
        lines = content.split('\n')
        line_num = self._get_line_number(content, position)
        
        start = max(0, line_num - 2)
        end = min(len(lines), line_num + 1)
        
        context_lines = []
        for i in range(start, end):
            prefix = ">>> " if i == line_num - 1 else "    "
            context_lines.append(f"{prefix}{i+1:3d}: {lines[i]}")
            
        return '\n'.join(context_lines)
    
    def check_foreign_keys(self) -> List[Dict]:
        """Check for foreign key constraints to non-order_service schemas"""
        fk_violations = []
        
        # Search for REFERENCES clauses in migrations
        migration_files = glob.glob("migrations/*.sql")
        
        for file_path in migration_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Find REFERENCES schema.table
                fk_pattern = r'REFERENCES\s+(\w+\.\w+)'
                matches = re.finditer(fk_pattern, content, re.IGNORECASE)
                
                for match in matches:
                    schema_table = match.group(1)
                    if '.' in schema_table:
                        schema, table = schema_table.split('.', 1)
                        
                        if schema.lower() not in self.valid_schemas:
                            fk_violations.append({
                                'file': file_path,
                                'line': self._get_line_number(content, match.start()),
                                'schema': schema,
                                'table': table,
                                'full_ref': schema_table,
                                'type': 'FOREIGN KEY',
                                'context': self._get_context(content, match.start())
                            })
                            
            except Exception as e:
                print(f"⚠️  Error scanning {file_path}: {e}")
        
        return fk_violations
    
    def suggest_api_replacement(self, table: str) -> str:
        """Suggest API replacement for a table"""
        for key, api in self.api_replacements.items():
            if key in table.lower():
                return api
        return "Appropriate Service API"
    
    def generate_report(self):
        """Generate comprehensive compliance report"""
        print("\n" + "="*80)
        print("🔒 ORDER_SERVICE SCHEMA COMPLIANCE CHECK")
        print("="*80)
        print(f"RULE: order_service can ONLY access order_service.* schema")
        print(f"❌ NO access to other schemas (public, user_service, backend, etc.)")
        
        # Check SQL violations
        print(f"\n1️⃣ SCANNING SQL QUERIES FOR SCHEMA VIOLATIONS...")
        sql_violations = self.scan_sql_queries()
        
        # Check FK violations  
        print(f"\n2️⃣ SCANNING FOREIGN KEY CONSTRAINTS...")
        fk_violations = self.check_foreign_keys()
        
        total_violations = len(sql_violations) + len(fk_violations)
        
        if total_violations == 0:
            print("\n🎉 ✅ COMPLIANCE ACHIEVED!")
            print("   All database access is properly isolated to order_service schema")
            return
            
        print(f"\n❌ FOUND {total_violations} SCHEMA VIOLATIONS")
        print("="*80)
        
        # Report SQL violations
        if sql_violations:
            print(f"\n📊 SQL QUERY VIOLATIONS ({len(sql_violations)}):")
            print("-" * 50)
            
            by_schema = {}
            for violation in sql_violations:
                schema = violation['schema']
                if schema not in by_schema:
                    by_schema[schema] = []
                by_schema[schema].append(violation)
            
            for schema, viols in by_schema.items():
                print(f"\n🚫 {schema}.* schema access ({len(viols)} violations):")
                for v in viols[:3]:  # Show first 3 violations per schema
                    api_suggestion = self.suggest_api_replacement(v['table'])
                    print(f"   📁 {v['file']}:{v['line']}")
                    print(f"   ❌ {v['sql_type']} {v['full_ref']}")
                    print(f"   ✅ FIX: Use {api_suggestion}")
                    print(f"   📝 Context:")
                    print("      " + v['context'].replace('\n', '\n      '))
                    print()
                
                if len(viols) > 3:
                    print(f"   ... and {len(viols) - 3} more violations in {schema} schema")
        
        # Report FK violations
        if fk_violations:
            print(f"\n🔗 FOREIGN KEY VIOLATIONS ({len(fk_violations)}):")
            print("-" * 50)
            
            for v in fk_violations:
                print(f"   📁 {v['file']}:{v['line']}")
                print(f"   ❌ FK constraint to {v['full_ref']}")
                print(f"   ✅ FIX: Remove FK, use API validation")
                print(f"   📝 Context:")
                print("      " + v['context'].replace('\n', '\n      '))
                print()
        
        # Provide fix guidance
        print("\n🛠️  REQUIRED FIXES:")
        print("="*80)
        print("1. Replace ALL SQL queries to other schemas with service API calls")
        print("2. Remove foreign key constraints to non-order_service tables")
        print("3. Implement API-based validation instead of DB constraints")
        print("4. Update migrations to only reference order_service schema")
        
        schemas_accessed = set(v['schema'] for v in sql_violations)
        if schemas_accessed:
            print(f"\n📋 API INTEGRATION NEEDED:")
            for schema in sorted(schemas_accessed):
                api = self.suggest_api_replacement(schema)
                print(f"   • {schema}.* → {api}")
        
        print("\n" + "="*80)


def main():
    """Run schema compliance check"""
    checker = SchemaComplianceChecker()
    checker.generate_report()


if __name__ == "__main__":
    main()