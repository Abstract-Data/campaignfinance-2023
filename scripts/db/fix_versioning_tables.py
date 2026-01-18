#!/usr/bin/env python3
"""
Fix the versioning table field constraints by replacing max_length with sa_column.
"""

import re

def fix_versioning_tables():
    """Fix all versioning table field constraints."""
    
    # Read the file
    with open('app/states/unified_sqlmodels.py', 'r') as f:
        content = f.read()
    
    # Replace all max_length constraints in versioning tables
    # Pattern: max_length=100 or max_length=255 in versioning table contexts
    content = re.sub(
        r'changed_by: Optional\[str\] = Field\(default=None, max_length=100\)',
        'changed_by: Optional[str] = Field(default=None, sa_column=Column(String(200)))',
        content
    )
    
    content = re.sub(
        r'change_reason: Optional\[str\] = Field\(default=None, max_length=255\)',
        'change_reason: Optional[str] = Field(default=None, sa_column=Column(String(500)))',
        content
    )
    
    # Also fix the UnifiedCommitteePerson table
    content = re.sub(
        r'last_modified_by: Optional\[str\] = Field\(default=None, max_length=100\)',
        'last_modified_by: Optional[str] = Field(default=None, sa_column=Column(String(200)))',
        content
    )
    
    # Write the fixed content back
    with open('app/states/unified_sqlmodels.py', 'w') as f:
        f.write(content)
    
    print("✅ Fixed all versioning table field constraints")

if __name__ == "__main__":
    fix_versioning_tables() 