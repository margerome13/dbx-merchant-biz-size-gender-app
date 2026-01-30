# Design Document

## Overview

This design adapts the existing Streamlit-based Databricks table editor to work with the `dg_dev.sandbox.out_merchant_business_size_for_bank` table. The solution leverages the existing architecture and patterns while removing MDAR-specific configurations and updating table references.

## Architecture

The application follows a single-page Streamlit architecture with the following components:

1. **Main Application** (`app.py`): Entry point that sets up navigation and page configuration
2. **View Registry** (`view_groups.py`): Configuration file that registers available views
3. **Editor View** (`views/merchant_business_size_editor.py`): The form-based editor interface
4. **Databricks Connection**: SQL warehouse connection using `databricks-sql-connector`

The architecture remains unchanged from the existing implementation, only the configuration and table references are modified.

## Components and Interfaces

### 1. View Registry Update

**File:** `view_groups.py`

**Changes:**
- Update view label from "DQ MDAR Masterfile Editor" to "Merchant Business Size Editor"
- Update help text to describe merchant business size table
- Update page reference to point to new view file

**Interface:**
```python
groups = [
    {
        "views": [
            {
                "label": "Merchant Business Size Editor",
                "help": "Form editor for the Merchant Business Size for Bank table.",
                "page": "views/merchant_business_size_editor.py",
                "icon": ":material/store:",
            },
        ],
    },
]
```

### 2. Editor View Implementation

**File:** `views/merchant_business_size_editor.py`

**Key Changes:**
- Remove MDAR-specific `DROPDOWN_VALUES` configuration
- Remove MDAR-specific `MULTILINE_FIELDS` configuration
- Remove `validate_ticket_format()` function (MDAR-specific)
- Remove `check_ticket_exists()` function (MDAR-specific)
- Simplify `validate_new_record()` to generic validation
- Update table connection constants
- Update page headers and descriptions
- Remove Manila timezone logic (unless needed for merchant data)

**Connection Configuration:**
```python
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"
TABLE_NAME = "dg_dev.sandbox.out_merchant_business_size_for_bank"
```

**Core Functions (Retained):**
- `get_connection()`: Establishes Databricks SQL connection
- `get_table_schema()`: Retrieves table schema dynamically
- `read_table()`: Reads table data with pagination
- `insert_record()`: Inserts new records
- `update_record()`: Updates existing records
- `delete_record()`: Deletes records
- `render_form_field()`: Dynamically renders form fields based on column type

**Simplified Validation:**
```python
def validate_new_record(record_data: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validate new record data.
    Returns: (is_valid, error_message)
    """
    # Check for empty required fields
    for field, value in record_data.items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return False, f"Field '{field}' cannot be empty."
    
    return True, ""
```

### 3. Git Integration

**Repository Configuration:**
- Remote URL: `https://github.com/margerome13/dbx-table-read-form-app-maker-checker`
- Branch: `main` (or current branch)

**Git Operations:**
1. Stage all modified files
2. Commit with descriptive message
3. Push to remote repository

## Data Models

The application works with dynamic data models based on the table schema retrieved from Databricks. No hardcoded data models are required.

**Schema Discovery:**
- Column names: Retrieved via `DESCRIBE TABLE` command
- Column types: Retrieved via `DESCRIBE TABLE` command
- Form field rendering: Based on column type (string, int, float, boolean, date, timestamp)

**Expected Table Structure:**
The application will adapt to whatever columns exist in `dg_dev.sandbox.out_merchant_business_size_for_bank`. Common merchant business size fields might include:
- Merchant identifiers
- Business size categories
- Bank-related fields
- Timestamps

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Table Connection Consistency
*For any* application startup, connecting to the database should always use the table name `dg_dev.sandbox.out_merchant_business_size_for_bank`

**Validates: Requirements 1.1**

### Property 2: Schema Reflection Completeness
*For any* table schema retrieved from Databricks, all columns in the schema should have corresponding form fields generated

**Validates: Requirements 3.1, 3.2**

### Property 3: CRUD Operation Correctness
*For any* valid record data, performing an insert operation should result in the record existing in the table when subsequently queried

**Validates: Requirements 4.1**

### Property 4: Update Idempotence
*For any* record, updating it with the same values twice should result in the same final state as updating it once

**Validates: Requirements 4.2**

### Property 5: Delete Completeness
*For any* record that exists in the table, deleting it should result in the record no longer appearing in subsequent queries

**Validates: Requirements 4.3**

### Property 6: Search Filter Correctness
*For any* search term, all returned records should contain the search term in at least one column

**Validates: Requirements 6.1**

### Property 7: Validation Rejection
*For any* record with empty required fields, validation should fail and prevent the database operation

**Validates: Requirements 5.1**

### Property 8: Configuration Cleanup
*For any* code review, no MDAR-specific constants or functions should remain in the merchant editor

**Validates: Requirements 8.1, 8.2, 8.3**

## Error Handling

### Connection Errors
- Display user-friendly error messages when Databricks connection fails
- Provide connection details for troubleshooting
- Maintain connection state in session to avoid repeated failures

### Validation Errors
- Display specific field-level errors when validation fails
- Prevent form submission until validation passes
- Highlight problematic fields in the UI

### Database Operation Errors
- Catch and display SQL errors with context
- Rollback failed operations (implicit in SQL connector)
- Provide actionable error messages

### Git Operation Errors
- Display clear messages if git operations fail
- Provide guidance on manual resolution
- Continue application functionality even if git push fails

## Testing Strategy

### Unit Tests
- Test `validate_new_record()` with various input combinations
- Test `render_form_field()` with different column types
- Test SQL query generation in CRUD functions
- Test error handling for edge cases

### Property-Based Tests
- Use `hypothesis` library for Python property-based testing
- Configure each test to run minimum 100 iterations
- Tag each test with format: **Feature: merchant-business-size-editor, Property {number}: {property_text}**

**Property Test Examples:**

```python
from hypothesis import given, strategies as st

@given(st.dictionaries(st.text(), st.text()))
def test_insert_then_query_returns_record(record_data):
    """Feature: merchant-business-size-editor, Property 3: CRUD Operation Correctness"""
    # Insert record
    insert_record(TABLE_NAME, record_data, conn)
    
    # Query for record
    result = read_table(TABLE_NAME, conn)
    
    # Assert record exists
    assert any(all(row[k] == v for k, v in record_data.items()) for row in result)
```

### Integration Tests
- Test full CRUD workflow: create → read → update → read → delete
- Test search functionality with various search terms
- Test form rendering with actual table schema
- Test connection establishment and error recovery

### Manual Testing
- Verify UI displays correct table name and headers
- Verify all form fields render correctly
- Verify CRUD operations work end-to-end
- Verify search and filter functionality
- Verify error messages are clear and helpful
