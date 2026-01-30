# CSV Upload to Databricks Table - Implementation Plan

## Overview
Based on the code from the databricks-apps-cookbook, we can implement a CSV upload feature that:
1. Uploads CSV files to a Unity Catalog Volume
2. Creates a Databricks Delta table from the CSV data
3. Integrates with your existing maker-checker workflow

## Reusable Code Components

### 1. From `volumes_upload.py`
**What it does:**
- Uploads files to Unity Catalog Volumes using Databricks SDK
- Validates volume permissions before upload
- Provides user feedback on upload status

**Key Functions:**
```python
def check_upload_permissions(volume_name: str):
    # Validates user has WRITE_VOLUME permissions
    # Returns permission status

# Upload process:
file_bytes = uploaded_file.read()
binary_data = io.BytesIO(file_bytes)
w.files.upload(volume_file_path, binary_data, overwrite=True)
```

### 2. From `tables_edit.py`
**What it does:**
- Connects to Databricks SQL warehouse
- Reads and writes data to Delta tables
- Uses parameterized queries for safe data insertion

**Key Functions:**
```python
@st.cache_resource(ttl=300)
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def insert_overwrite_table(table_name: str, df: pd.DataFrame, conn):
    # Inserts DataFrame into Delta table
    # Uses parameterized queries for safety
```

## How It Will Work

### Step-by-Step Workflow

#### Phase 1: CSV Upload to Volume
1. **User uploads CSV file** via Streamlit file uploader
2. **System validates**:
   - File is CSV format
   - User has permissions to target volume
   - CSV structure is valid
3. **File is uploaded** to Unity Catalog Volume (e.g., `main.sandbox.csv_uploads/`)
4. **System reads CSV** into pandas DataFrame for preview

#### Phase 2: Table Creation/Update
1. **User specifies target table** (catalog.schema.table_name)
2. **System checks** if table exists:
   - **If exists**: Option to append or overwrite
   - **If new**: Create new Delta table
3. **Schema mapping**:
   - Auto-detect column types from CSV
   - Allow user to adjust column names/types
4. **Data validation**:
   - Check for required columns (business_reviewed_size_pending, etc.)
   - Validate data types
5. **Write to Delta table** using SQL INSERT or CREATE TABLE AS SELECT

#### Phase 3: Integration with Maker-Checker
1. **Uploaded data** starts with `review_status = NULL` or `PENDING`
2. **Makers can review** uploaded records
3. **Checkers approve** as normal workflow

## Implementation Code Structure

### New File: `views/csv_upload_to_table.py`

```python
import io
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Configuration
UPLOAD_VOLUME = "dg_dev.sandbox.csv_uploads"  # Unity Catalog Volume
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"

w = WorkspaceClient()

def upload_csv_to_volume(uploaded_file, volume_path):
    """Upload CSV to Unity Catalog Volume"""
    file_bytes = uploaded_file.read()
    binary_data = io.BytesIO(file_bytes)
    file_name = uploaded_file.name
    
    parts = volume_path.strip().split(".")
    catalog, schema, volume_name = parts[0], parts[1], parts[2]
    volume_file_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{file_name}"
    
    w.files.upload(volume_file_path, binary_data, overwrite=True)
    return volume_file_path

def create_table_from_csv(df: pd.DataFrame, table_name: str, conn, mode="create"):
    """Create or append to Delta table from DataFrame"""
    with conn.cursor() as cursor:
        if mode == "create":
            # Infer schema from DataFrame
            columns_def = []
            for col, dtype in df.dtypes.items():
                if dtype == 'object':
                    sql_type = 'STRING'
                elif dtype == 'int64':
                    sql_type = 'BIGINT'
                elif dtype == 'float64':
                    sql_type = 'DOUBLE'
                elif dtype == 'bool':
                    sql_type = 'BOOLEAN'
                else:
                    sql_type = 'STRING'
                columns_def.append(f"`{col}` {sql_type}")
            
            # Create table
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_def)}
            ) USING DELTA
            """
            cursor.execute(create_sql)
        
        # Insert data
        rows = list(df.itertuples(index=False, name=None))
        if not rows:
            return
        
        cols = list(df.columns)
        params = {}
        values_sql_parts = []
        p = 0
        
        for row in rows:
            ph = []
            for v in row:
                key = f"p{p}"
                ph.append(f":{key}")
                params[key] = v
                p += 1
            values_sql_parts.append("(" + ",".join(ph) + ")")
        
        values_sql = ",".join(values_sql_parts)
        col_list_sql = ",".join([f"`{col}`" for col in cols])
        
        if mode == "append":
            cursor.execute(
                f"INSERT INTO {table_name} ({col_list_sql}) VALUES {values_sql}",
                params
            )
        else:
            cursor.execute(
                f"INSERT OVERWRITE {table_name} ({col_list_sql}) VALUES {values_sql}",
                params
            )

# Streamlit UI
st.header("CSV Upload to Databricks Table", divider=True)
st.subheader("Upload CSV and Create/Update Delta Table")

tab1, tab2 = st.tabs(["Upload CSV", "Configuration"])

with tab1:
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
    
    if uploaded_file:
        # Preview CSV
        df = pd.read_csv(uploaded_file)
        st.write("**CSV Preview:**")
        st.dataframe(df.head(10))
        st.info(f"Rows: {len(df)} | Columns: {len(df.columns)}")
        
        # Target table selection
        target_table = st.text_input(
            "Target Table Name:",
            placeholder="dg_dev.sandbox.out_merchant_business_size_for_bank",
            help="Format: catalog.schema.table_name"
        )
        
        # Upload mode
        upload_mode = st.radio(
            "Upload Mode:",
            ["Create New Table", "Append to Existing", "Overwrite Existing"],
            help="Choose how to handle the data"
        )
        
        # Column mapping (optional)
        with st.expander("Column Mapping (Optional)"):
            st.write("Map CSV columns to required table columns:")
            col1, col2 = st.columns(2)
            with col1:
                st.write("**CSV Columns:**")
                for col in df.columns:
                    st.text(col)
            with col2:
                st.write("**Required Columns:**")
                required_cols = [
                    "business_reviewed_size_pending",
                    "business_reviewed_gender_pending",
                    "review_status"
                ]
                for col in required_cols:
                    st.text(col)
        
        # Upload button
        if st.button("Upload and Create Table", type="primary"):
            if not target_table:
                st.error("Please specify a target table name")
            else:
                try:
                    with st.spinner("Processing..."):
                        # Step 1: Upload to volume
                        st.info("📤 Uploading CSV to volume...")
                        volume_path = upload_csv_to_volume(uploaded_file, UPLOAD_VOLUME)
                        
                        # Step 2: Create/update table
                        st.info("📊 Creating/updating Delta table...")
                        conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                        
                        mode_map = {
                            "Create New Table": "create",
                            "Append to Existing": "append",
                            "Overwrite Existing": "create"
                        }
                        
                        create_table_from_csv(
                            df, 
                            target_table, 
                            conn, 
                            mode=mode_map[upload_mode]
                        )
                        
                        st.success(f"✅ Successfully created table: {target_table}")
                        st.success(f"✅ CSV uploaded to: {volume_path}")
                        st.balloons()
                        
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

with tab2:
    st.write("**Configuration Settings:**")
    st.code(f"""
Upload Volume: {UPLOAD_VOLUME}
Databricks Host: {DATABRICKS_HOST}
SQL Warehouse Path: {HTTP_PATH}
    """)
```

## Required Permissions

### For App Service Principal:
1. **Volume Permissions:**
   - `USE CATALOG` on the catalog
   - `USE SCHEMA` on the schema
   - `READ VOLUME` and `WRITE VOLUME` on the volume

2. **Table Permissions:**
   - `CREATE TABLE` on the schema
   - `MODIFY` on existing tables (for append/overwrite)
   - `SELECT` on tables (for validation)

3. **SQL Warehouse:**
   - `CAN USE` on the SQL warehouse

## Benefits of This Approach

1. **Two-Stage Process:**
   - CSV stored in Volume (backup/audit trail)
   - Data loaded into Delta table (queryable)

2. **Flexibility:**
   - Support create, append, or overwrite modes
   - Column mapping for different CSV formats
   - Preview before upload

3. **Integration:**
   - Works with existing maker-checker workflow
   - Uses same connection patterns
   - Maintains audit trail

4. **Safety:**
   - Parameterized queries prevent SQL injection
   - Permission validation before upload
   - Preview data before committing

## Next Steps

1. **Add to view_groups.py:**
```python
{
    "label": "CSV Upload to Table",
    "help": "Upload CSV files and create Databricks Delta tables",
    "page": "views/csv_upload_to_table.py",
    "icon": ":material/upload_file:",
}
```

2. **Test with sample CSV** containing required columns

3. **Add validation** for required columns in maker-checker workflow

4. **Consider adding:**
   - Data type conversion options
   - Column renaming interface
   - Batch upload support
   - Upload history tracking
