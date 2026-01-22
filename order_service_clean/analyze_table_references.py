#!/usr/bin/env python3
"""
Table Reference Analysis

Analyzes the codebase to identify table references and provide insights
for database consistency checking. This runs without database connection.
"""

import re
import glob
from typing import Dict, List, Set, Tuple
from collections import defaultdict

class TableReferenceAnalyzer:
    """Analyzes table references in the codebase"""
    
    def __init__(self):
        self.order_service_refs = set()
        self.other_schema_refs = defaultdict(set)
        self.file_table_map = defaultdict(list)
        
    def find_table_references(self) -> Dict[str, Set[str]]:
        """Find all table references in the codebase"""
        
        # Search Python files and SQL migrations
        file_patterns = ["app/**/*.py", "migrations/*.sql"]
        
        # Patterns to find table references
        patterns = [
            # Qualified table references
            r'(\w+)\.(\w+)',  # schema.table
            # SQLAlchemy table names
            r'__tablename__\s*=\s*["\'](\w+)["\']',
            # Unqualified table references in SQL context
            r'FROM\s+(\w+)(?:\s|$)',
            r'JOIN\s+(\w+)(?:\s|$)',
            r'INSERT\s+INTO\s+(\w+)(?:\s|$)',
            r'UPDATE\s+(\w+)(?:\s|$)',
            r'DELETE\s+FROM\s+(\w+)(?:\s|$)',
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)(?:\s|$)',
            r'ALTER\s+TABLE\s+(\w+)(?:\s|$)',
        ]
        
        for pattern in file_patterns:
            files = glob.glob(pattern, recursive=True)
            
            for file_path in files:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # Find qualified schema.table references
                    for match in re.finditer(r'(\w+)\.(\w+)', content, re.IGNORECASE):
                        schema = match.group(1).lower()
                        table = match.group(2).lower()
                        
                        # Skip obvious non-table patterns
                        if self._is_likely_table_reference(schema, table, content, match.start()):
                            full_ref = f"{schema}.{table}"
                            
                            if schema == 'order_service':
                                self.order_service_refs.add(table)
                            else:
                                self.other_schema_refs[schema].add(table)
                            
                            self.file_table_map[file_path].append({
                                'type': 'qualified',
                                'schema': schema,
                                'table': table,
                                'full_ref': full_ref,
                                'line': self._get_line_number(content, match.start())
                            })
                    
                    # Find __tablename__ references
                    for match in re.finditer(r'__tablename__\s*=\s*["\'](\w+)["\']', content, re.IGNORECASE):
                        table = match.group(1).lower()
                        self.order_service_refs.add(table)  # Assume these are order_service tables
                        
                        self.file_table_map[file_path].append({
                            'type': 'tablename',
                            'schema': 'order_service',
                            'table': table,
                            'full_ref': f"order_service.{table}",
                            'line': self._get_line_number(content, match.start())
                        })
                    
                except Exception as e:
                    print(f"⚠️  Error scanning {file_path}: {e}")
                    
        return {
            'order_service': self.order_service_refs,
            'other_schemas': dict(self.other_schema_refs)
        }
    
    def _is_likely_table_reference(self, schema: str, table: str, content: str, position: int) -> bool:
        """Check if this is likely a table reference vs. other qualified names"""
        
        # Get context around the match
        start = max(0, position - 50)
        end = min(len(content), position + 100)
        context = content[start:end].lower()
        
        # Skip common false positives
        false_positive_schemas = {
            'datetime', 'json', 'os', 'sys', 'logging', 're', 'typing',
            'sqlalchemy', 'fastapi', 'pydantic', 'httpx', 'asyncio',
            'decimal', 'uuid', 'enum', 'collections', 'functools',
            'pathlib', 'urllib', 'time', 'math', 'random', 'string',
            'itertools', 'operator', 'threading', 'multiprocessing'
        }
        
        false_positive_tables = {
            'execute', 'fetchone', 'fetchall', 'commit', 'rollback',
            'close', 'connect', 'session', 'query', 'select', 'insert',
            'update', 'delete', 'create', 'drop', 'alter', 'index',
            'constraint', 'foreign', 'primary', 'key', 'references',
            'get', 'post', 'put', 'delete', 'patch', 'head', 'options',
            'json', 'text', 'status_code', 'headers', 'cookies', 'params'
        }
        
        if schema in false_positive_schemas or table in false_positive_tables:
            return False
        
        # Look for SQL context
        sql_indicators = [
            'from', 'join', 'insert', 'into', 'update', 'delete',
            'create', 'table', 'alter', 'drop', 'select', 'where',
            'group', 'order', 'by', 'having', 'limit', 'offset'
        ]
        
        # Higher likelihood if in SQL context
        if any(indicator in context for indicator in sql_indicators):
            return True
        
        # Check for schema names that are likely databases
        database_schemas = {
            'public', 'order_service', 'user_service', 'algo_engine',
            'backend', 'signal_service', 'analytics_service'
        }
        
        if schema in database_schemas:
            return True
        
        # Default: might be a table reference
        return len(schema) > 2 and len(table) > 2 and '_' in table
    
    def _get_line_number(self, content: str, position: int) -> int:
        """Get line number for a position in content"""
        return content[:position].count('\\n') + 1
    
    def generate_analysis_report(self):
        """Generate comprehensive analysis report"""
        print("\\n" + "="*80)
        print("📊 TABLE REFERENCE ANALYSIS REPORT")
        print("="*80)
        print("Analysis of table references found in codebase")
        
        # Find all references
        references = self.find_table_references()
        
        print(f"\\n1️⃣ ORDER_SERVICE SCHEMA TABLES:")
        print(f"Found {len(references['order_service'])} table references:")
        for table in sorted(references['order_service']):
            print(f"  📄 order_service.{table}")
        
        print(f"\\n2️⃣ OTHER SCHEMA REFERENCES:")
        total_other_refs = sum(len(tables) for tables in references['other_schemas'].values())
        print(f"Found {total_other_refs} references to {len(references['other_schemas'])} other schemas:")
        
        for schema in sorted(references['other_schemas'].keys()):
            tables = references['other_schemas'][schema]
            print(f"\\n🔍 {schema} schema ({len(tables)} tables):")
            for table in sorted(tables):
                print(f"    • {schema}.{table}")
        
        print(f"\\n" + "="*80)
        print("📋 DATABASE CONSISTENCY RECOMMENDATIONS")
        print("="*80)
        
        print(f"\\n✅ TABLES THAT SHOULD EXIST IN order_service:")
        if references['order_service']:
            for table in sorted(references['order_service']):
                print(f"  • order_service.{table}")
        else:
            print(f"  (No order_service tables found in code)")
        
        print(f"\\n❌ CROSS-SCHEMA VIOLATIONS TO FIX:")
        if references['other_schemas']:
            for schema, tables in references['other_schemas'].items():
                if schema not in ['information_schema']:  # Exclude system schemas
                    print(f"\\n  🚫 {schema} schema violations:")
                    for table in sorted(tables):
                        print(f"    • Replace {schema}.{table} access with {schema.title()} Service API")
        else:
            print(f"  ✅ No cross-schema violations found!")
        
        # File-by-file breakdown
        print(f"\\n📂 FILES WITH TABLE REFERENCES:")
        violation_files = []
        clean_files = []
        
        for file_path, refs in self.file_table_map.items():
            has_violations = any(ref['schema'] != 'order_service' and ref['schema'] != 'information_schema' 
                               for ref in refs)
            if has_violations:
                violation_files.append((file_path, refs))
            elif refs:  # Has references but no violations
                clean_files.append((file_path, refs))
        
        if violation_files:
            print(f"\\n🚨 Files with schema violations ({len(violation_files)}):")
            for file_path, refs in violation_files[:10]:  # Show first 10
                violations = [ref for ref in refs 
                            if ref['schema'] not in ['order_service', 'information_schema']]
                print(f"  📁 {file_path}")
                for violation in violations[:3]:  # Show first 3 violations per file
                    print(f"    ❌ Line {violation['line']}: {violation['full_ref']}")
                if len(violations) > 3:
                    print(f"    ... and {len(violations) - 3} more violations")
        
        if clean_files:
            print(f"\\n✅ Files with only valid references ({len(clean_files)}):")
            for file_path, refs in clean_files[:5]:  # Show first 5
                valid_refs = [ref for ref in refs if ref['schema'] == 'order_service']
                print(f"  📁 {file_path} ({len(valid_refs)} order_service refs)")
        
        # Summary statistics
        print(f"\\n📊 SUMMARY STATISTICS:")
        print(f"  • Total files analyzed: {len(self.file_table_map)}")
        print(f"  • Files with violations: {len(violation_files)}")
        print(f"  • Files with clean references: {len(clean_files)}")
        print(f"  • order_service tables referenced: {len(references['order_service'])}")
        print(f"  • Other schemas accessed: {len(references['other_schemas'])}")
        print(f"  • Total violation count: {total_other_refs}")
        
        if total_other_refs == 0:
            print(f"\\n🎉 ✅ PERFECT SCHEMA ISOLATION!")
            print(f"   All table access is properly isolated to order_service schema")
        else:
            compliance_score = len(references['order_service']) / (len(references['order_service']) + total_other_refs) * 100
            print(f"\\n📈 Schema compliance score: {compliance_score:.1f}%")
        
        print(f"\\n" + "="*80)


def main():
    """Run table reference analysis"""
    analyzer = TableReferenceAnalyzer()
    analyzer.generate_analysis_report()


if __name__ == "__main__":
    main()