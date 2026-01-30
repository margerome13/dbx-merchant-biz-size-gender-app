# CSV Upload Feature - How It Works

## Quick Summary

The CSV upload feature combines two powerful patterns from the Databricks Apps Cookbook:
1. **File Upload** (from `volumes_upload.py`) - Uploads files to Unity Catalog Volumes
2. **Table Operations** (from `tables_edit.py`) - Creates and manages Delta tables

## The Two-Stage Process

### Stage 1: Upload CSV to Volume (Storage Layer)
```
User's Computer → Streamlit App → Unity Catalog Volume
     (CSV)           (Python)         (Cloud Storage)
```

**What happens:**
- User selects CSV file via Streamlit's `file_uploader`
- File is read into memory as bytes
- Databricks SDK uploads to Volume path like: `/Volumes/catalog/schema/volume/filename.csv`
- This creates a permanent backup of the original CSV

**Code from cookbook:**
```python
file_bytes = uploaded_file.read()
binary_data = io.BytesIO(file_bytes)
w.files.upload(volume_file_path, binary_data, overwrite=True)
```

### Stage 2: Create Delta Table (Query Layer)
```
CSV Data → Pandas DataFrame → SQL INSERT → Delta Table
(Volume)      (Memory)         (SQL)      (Queryable)
```

**What happens:**
- CSV is read into pandas DataFrame for processing
- System infers column types (STRING, BIGINT, DOUBLE, etc.)
- Creates Delta table with proper schema
- Inserts data using parameterized SQL queries
- Table is now queryable via SQL

**Code from cookbook:**
```python
# Read CSV
df = pd.read_csv(uploaded_file)

# Create table structure
CREATE TABLE catalog.schema.table (
    column1 STRING,
    column2 BIGINT,
    ...
) USING DELTA

# Insert data safely
INSERT INTO table (col1, col2) VALUES (:p0, :p1), (:p2, :p3), ...
```

## Why This Approach?

### 1. Separation of Concerns
- **Volume**: Raw file storage (backup, audit trail)
- **Table**: Structured data (fast queries, analytics)

### 2. Safety Features
- **Permission checks** before upload
- **Parameterized queries** prevent SQL injection
- **Preview data** before committing
- **Overwrite protection** with mode selection

### 3. Flexibility
- **Create new tables** from scratch
- **Append** to existing tables
- **Overwrite** existing data
- **Column mapping** for different CSV formats

## Real-World Example

Let's say you have a CSV file `merchants_jan_2026.csv`:
```csv
merchant_id,business_size,gender,revenue
M001,SMALL,MALE,50000
M002,MEDIUM,FEMALE,150000
M003,MICRO,MALE,10000
```

### Step 1: Upload to Volume
```
File uploaded to:
/Volumes/dg_dev/sandbox/csv_uploads/merchants_jan_2026.csv
```

### Step 2: Create Table
```sql
CREATE TABLE dg_dev.sandbox.merchant_data (
    merchant_id STRING,
    business_size STRING,
    gender STRING,
    revenue BIGINT
) USING DELTA;

INSERT INTO dg_dev.sandbox.merchant_data 
VALUES ('M001', 'SMALL', 'MALE', 50000),
       ('M002', 'MEDIUM', 'FEMALE', 150000),
       ('M003', 'MICRO', 'MALE', 10000);
```

### Step 3: Now Available for Maker-Checker
```sql
-- Makers can now review these records
SELECT * FROM dg_dev.sandbox.merchant_data 
WHERE review_status IS NULL;
```

## Integration with Your App

### Current Flow:
```
Maker → Select Record → Fill Form → Submit → Checker Reviews
```

### With CSV Upload:
```
CSV Upload → Create Table → Maker Reviews → Submit → Checker Approves
     ↓
  (Bulk data entry instead of manual record-by-record)
```

## Key Components Reused

### From `volumes_upload.py`:
1. **Permission validation**
   ```python
   def check_upload_permissions(volume_name: str):
       # Checks if user has WRITE_VOLUME privilege
   ```

2. **File upload mechanism**
   ```python
   w.files.upload(volume_file_path, binary_data, overwrite=True)
   ```

### From `tables_edit.py`:
1. **SQL connection management**
   ```python
   @st.cache_resource(ttl=300)
   def get_connection(http_path):
       return sql.connect(...)
   ```

2. **Safe data insertion**
   ```python
   def insert_overwrite_table(table_name: str, df: pd.DataFrame, conn):
       # Uses parameterized queries with :p0, :p1, etc.
   ```

## Technical Flow Diagram

```
┌─────────────────┐
│  User uploads   │
│   CSV file      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Streamlit      │
│  file_uploader  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│  Read CSV into  │────▶│  Preview data    │
│  pandas.DataFrame│     │  Show columns    │
└────────┬────────┘     └──────────────────┘
         │
         ▼
┌─────────────────┐
│  Upload to      │
│  Unity Catalog  │
│  Volume         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Infer schema   │
│  from DataFrame │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CREATE TABLE   │
│  (if new)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  INSERT data    │
│  using params   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Delta table    │
│  ready for      │
│  maker-checker  │
└─────────────────┘
```

## Error Handling

The implementation includes robust error handling:

1. **File validation**: Check CSV format
2. **Permission checks**: Verify user can write to volume/table
3. **Schema validation**: Ensure columns match requirements
4. **SQL errors**: Catch and display connection/query errors
5. **Data type errors**: Handle type conversion issues

## Performance Considerations

- **Batch inserts**: Uses parameterized queries for efficiency
- **Connection caching**: Reuses SQL connections (TTL: 5 minutes)
- **Preview limits**: Shows only first 10 rows for preview
- **Streaming**: Large files handled in chunks if needed

## Security Features

1. **Parameterized queries**: Prevents SQL injection
   ```python
   # Safe: Uses :p0, :p1 parameters
   cursor.execute("INSERT INTO table VALUES (:p0, :p1)", {"p0": val1, "p1": val2})
   
   # Unsafe: Direct string interpolation (NOT USED)
   # cursor.execute(f"INSERT INTO table VALUES ('{val1}', '{val2}')")
   ```

2. **Permission validation**: Checks before upload
3. **Audit trail**: Files stored in Volume with timestamps
4. **User tracking**: Databricks authentication tracks who uploaded

## Summary

This CSV upload feature is essentially:
1. **Taking the file upload code** from `volumes_upload.py`
2. **Combining it with table operations** from `tables_edit.py`
3. **Adding CSV parsing** with pandas
4. **Creating a user-friendly interface** for bulk data entry

The result is a powerful tool that lets users upload hundreds or thousands of merchant records at once, instead of entering them one by one through the maker interface.
