import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import pytz

# Pre-configured connection details
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"

# Available tables
AVAILABLE_TABLES = {
    "Dev - Merchant Business Size": "dg_dev.sandbox.out_merchant_business_size_for_bank",
    "Prod Test - Merchant Business Size": "dg_prod.sandbox.out_merchant_business_size_for_bank_test"
}

# Dropdown values for review fields
BUSINESS_SIZE_OPTIONS = ["", "MICRO", "SMALL", "MEDIUM", "LARGE"]
GENDER_OPTIONS = ["", "MALE", "FEMALE"]

# Initialize session state
if 'selected_record' not in st.session_state:
    st.session_state.selected_record = None
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'table_data' not in st.session_state:
    st.session_state.table_data = None
if 'table_schema' not in st.session_state:
    st.session_state.table_schema = None
if 'connection_established' not in st.session_state:
    st.session_state.connection_established = False
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = list(AVAILABLE_TABLES.keys())[0]
if 'current_table_name' not in st.session_state:
    st.session_state.current_table_name = AVAILABLE_TABLES[list(AVAILABLE_TABLES.keys())[0]]

@st.cache_resource(ttl="1h")
def get_connection(server_hostname: str, http_path: str):
    """Create connection to Databricks SQL warehouse"""
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: Config().authenticate,
    )

def get_current_user_email() -> str:
    """Get the current user's Databricks email - no caching to ensure fresh data"""
    try:
        # Method 1: Try SQL query to get current user (most reliable in Databricks Apps)
        try:
            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_user()")
                result = cursor.fetchone()
                if result and result[0]:
                    user_value = result[0]
                    # If it's an email, return it
                    if '@' in str(user_value):
                        return str(user_value)
        except Exception as sql_error:
            st.warning(f"SQL method failed: {str(sql_error)}")
        
        # Method 2: Try Streamlit's experimental user info
        if hasattr(st, 'experimental_user') and st.experimental_user:
            user_email = st.experimental_user.get('email')
            if user_email and '@' in user_email:
                return user_email
        
        # Method 3: Try WorkspaceClient
        w = WorkspaceClient()
        current_user = w.current_user.me()
        
        # Check if we got a user_name (email)
        if current_user.user_name and '@' in str(current_user.user_name):
            return current_user.user_name
        
        # Try to get email from emails array
        if hasattr(current_user, 'emails') and current_user.emails and len(current_user.emails) > 0:
            email_value = current_user.emails[0].value
            if email_value and '@' in email_value:
                return email_value
        
        # Try display name
        if hasattr(current_user, 'display_name') and current_user.display_name and '@' in current_user.display_name:
            return current_user.display_name
        
        # If we only have an ID, show a warning and return it
        user_id = str(current_user.id) if hasattr(current_user, 'id') and current_user.id else "unknown"
        st.warning(f"⚠️ Could not retrieve email. Using user ID: {user_id}. Please contact your Databricks administrator.")
        return user_id
        
    except Exception as e:
        error_msg = f"Error retrieving user: {str(e)}"
        st.error(error_msg)
        return "unknown@databricks.com"

def get_manila_timestamp() -> str:
    """Get current timestamp in Manila timezone"""
    manila_tz = pytz.timezone('Asia/Manila')
    manila_time = datetime.now(manila_tz)
    return manila_time.strftime('%Y-%m-%d %H:%M:%S')

# Page header and user info
st.header(body="Merchant Business Size and Gender Review", divider=True)
st.subheader("Review and Update Merchant Data")
st.write(
    "Review merchant records and provide **business_reviewed_size** (MICRO, SMALL, MEDIUM, LARGE) and **business_reviewed_gender** (MALE, FEMALE)."
)

# Display current user with debug info
current_user = get_current_user_email()
if '@' in current_user:
    st.info(f"👤 **Logged in as:** {current_user}")
else:
    st.warning(f"⚠️ **User ID (not email):** {current_user} - Records will be saved with this ID. Contact your admin to configure email access.")

def get_table_schema(table_name: str, conn) -> Dict[str, str]:
    """Get table schema information"""
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema_info = cursor.fetchall()
        return {row[0]: row[1] for row in schema_info}

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

def read_table(table_name: str, conn, limit: int = 1000) -> pd.DataFrame:
    """Read table data with optional limit"""
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def insert_record(table_name: str, record_data: Dict[str, Any], conn):
    """Insert a new record"""
    # Clean up placeholder values
    for key, value in record_data.items():
        if value == "-- Select --":
            record_data[key] = ""
    
    columns = list(record_data.keys())
    values = []
    
    for val in record_data.values():
        if val is None or val == "":
            values.append("NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")  # Escape single quotes
            values.append(f"'{escaped_val}'")
        else:
            values.append(str(val))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        cursor.execute(query)

def update_record(table_name: str, record_data: Dict[str, Any], where_clause: str, conn):
    """Update an existing record"""
    set_clauses = []
    
    for col, val in record_data.items():
        if val is None or val == "":
            set_clauses.append(f"{col} = NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")  # Escape single quotes
            set_clauses.append(f"{col} = '{escaped_val}'")
        else:
            set_clauses.append(f"{col} = {val}")
    
    set_clause = ", ".join(set_clauses)
    
    with conn.cursor() as cursor:
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        cursor.execute(query)

def delete_record(table_name: str, where_clause: str, conn):
    """Delete a record"""
    with conn.cursor() as cursor:
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(query)

def render_form_field(column_name: str, column_type: str, current_value: Any = None, key_suffix: str = ""):
    """Render appropriate form field based on column type"""
    if current_value is None:
        current_value = ""
    
    # Handle pandas NaN values
    if pd.isna(current_value):
        current_value = ""
    
    field_key = f"{column_name}_{key_suffix}" if key_suffix else column_name
    
    # Special handling for review fields with dropdowns
    if column_name == "business_reviewed_size":
        current_str = str(current_value) if current_value != "" and current_value is not None else ""
        try:
            default_index = BUSINESS_SIZE_OPTIONS.index(current_str) if current_str in BUSINESS_SIZE_OPTIONS else 0
        except ValueError:
            default_index = 0
        return st.selectbox(
            f"{column_name} ({column_type})",
            options=BUSINESS_SIZE_OPTIONS,
            index=default_index,
            key=field_key,
            help="Select business size: MICRO, SMALL, MEDIUM, or LARGE"
        )
    elif column_name == "business_reviewed_gender":
        current_str = str(current_value) if current_value != "" and current_value is not None else ""
        try:
            default_index = GENDER_OPTIONS.index(current_str) if current_str in GENDER_OPTIONS else 0
        except ValueError:
            default_index = 0
        return st.selectbox(
            f"{column_name} ({column_type})",
            options=GENDER_OPTIONS,
            index=default_index,
            key=field_key,
            help="Select gender: MALE or FEMALE"
        )
    
    # Convert column type to appropriate Streamlit input
    if "int" in column_type.lower() or "bigint" in column_type.lower():
        try:
            default_val = int(current_value) if current_value != "" and current_value is not None else 0
        except (ValueError, TypeError):
            default_val = 0
        return st.number_input(
            f"{column_name} ({column_type})",
            value=default_val,
            step=1,
            key=field_key
        )
    elif "float" in column_type.lower() or "double" in column_type.lower() or "decimal" in column_type.lower():
        try:
            default_val = float(current_value) if current_value != "" and current_value is not None else 0.0
        except (ValueError, TypeError):
            default_val = 0.0
        return st.number_input(
            f"{column_name} ({column_type})",
            value=default_val,
            step=0.01,
            key=field_key
        )
    elif "boolean" in column_type.lower():
        try:
            default_val = bool(current_value) if current_value != "" and current_value is not None else False
        except (ValueError, TypeError):
            default_val = False
        return st.checkbox(
            f"{column_name} ({column_type})",
            value=default_val,
            key=field_key
        )
    elif "date" in column_type.lower() and "timestamp" not in column_type.lower():
        return st.date_input(
            f"{column_name} ({column_type})",
            key=field_key
        )
    elif "timestamp" in column_type.lower():
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            help="Format: YYYY-MM-DD HH:MM:SS",
            key=field_key
        )
    else:  # Default to text input for strings and other types
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            key=field_key
        )

# Main interface
tab_form, tab_view = st.tabs(["**Form Editor**", "**Table View**"])

with tab_form:
    # Table selector
    st.write("### Select Table")
    selected_table_name = st.selectbox(
        "Choose a table to review:",
        options=list(AVAILABLE_TABLES.keys()),
        index=list(AVAILABLE_TABLES.keys()).index(st.session_state.selected_table),
        key="table_selector"
    )
    
    # Update selected table if changed
    if selected_table_name != st.session_state.selected_table:
        st.session_state.selected_table = selected_table_name
        st.session_state.current_table_name = AVAILABLE_TABLES[selected_table_name]
        st.session_state.connection_established = False
        st.session_state.table_data = None
        st.session_state.table_schema = None
    
    TABLE_NAME = st.session_state.current_table_name
    
    # Connection info display
    st.info(f"🔗 **Connection Details:**\n- Host: `{DATABRICKS_HOST}`\n- Warehouse: `{HTTP_PATH}`\n- Table: `{TABLE_NAME}`")
    
    # Connect button
    if st.button("🔌 Connect to Table", type="primary"):
        try:
            with st.spinner("Connecting to Databricks..."):
                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                st.session_state.table_data = read_table(TABLE_NAME, conn)
                st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
                st.session_state.connection_established = True
            st.success("✅ Successfully connected to the table!")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")
            st.session_state.connection_established = False
    
    # Show connection status
    if st.session_state.connection_established and st.session_state.table_data is not None:
        st.success(f"✅ Connected to {TABLE_NAME} ({len(st.session_state.table_data)} records)")
        
        # Refresh button
        if st.button("🔄 Refresh Data"):
            try:
                with st.spinner("Refreshing data..."):
                    conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                    st.session_state.table_data = read_table(TABLE_NAME, conn)
                    st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
                st.success("✅ Data refreshed!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Refresh failed: {str(e)}")
        
        # Edit Record Section (removed action selection)
        st.subheader("📝 Review and Update Record")
        
        # Record selection
        if len(st.session_state.table_data) > 0:
                # Create more readable record options with a placeholder
                record_options = ["-- Select a record to edit --"]
                for i, row in st.session_state.table_data.iterrows():
                    # Show first few non-null columns for identification
                    display_cols = []
                    for col, val in row.items():
                        if pd.notna(val) and val != "" and len(display_cols) < 3:
                            display_cols.append(f"{col}={val}")
                    record_display = f"Row {i}: {' | '.join(display_cols)}"
                    record_options.append(record_display)
                
                selected_option = st.selectbox(
                    "Select record to edit:",
                    range(len(record_options)),
                    format_func=lambda x: record_options[x],
                    index=0
                )
                
                # Only show form if a valid record is selected (not the placeholder)
                if selected_option > 0:
                    selected_idx = selected_option - 1  # Adjust for placeholder
                    selected_record = st.session_state.table_data.iloc[selected_idx]
                    
                    # Show current record in expandable section
                    with st.expander("📋 Current Record Details", expanded=True):
                        # Display all fields as read-only except the review fields
                        for column, value in selected_record.items():
                            if column not in ["business_reviewed_size", "business_reviewed_gender"]:
                                st.text(f"{column}: {value}")
                    
                    # Form for editing ONLY review fields
                    with st.form("edit_record_form"):
                        st.write("**Review Fields (Editable):**")
                        st.info("ℹ️ Only business_reviewed_size and business_reviewed_gender can be edited.")
                        
                        form_data = {}
                        
                        # Copy all existing values
                        for column in st.session_state.table_schema.keys():
                            form_data[column] = selected_record.get(column, "")
                        
                        # Create editable fields only for review columns
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if "business_reviewed_size" in st.session_state.table_schema:
                                current_value = selected_record.get("business_reviewed_size", "")
                                dtype = st.session_state.table_schema["business_reviewed_size"]
                                form_data["business_reviewed_size"] = render_form_field(
                                    "business_reviewed_size", 
                                    dtype, 
                                    current_value, 
                                    "edit"
                                )
                        
                        with col2:
                            if "business_reviewed_gender" in st.session_state.table_schema:
                                current_value = selected_record.get("business_reviewed_gender", "")
                                dtype = st.session_state.table_schema["business_reviewed_gender"]
                                form_data["business_reviewed_gender"] = render_form_field(
                                    "business_reviewed_gender", 
                                    dtype, 
                                    current_value, 
                                    "edit"
                                )
                        
                        col_save, col_cancel = st.columns(2)
                        with col_save:
                            save_changes = st.form_submit_button("💾 Save Changes", type="primary")
                        with col_cancel:
                            cancel_changes = st.form_submit_button("❌ Cancel")
                        
                        if save_changes:
                            try:
                                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                # Create WHERE clause using first column as identifier
                                first_col = list(st.session_state.table_schema.keys())[0]
                                first_val = selected_record[first_col]
                                if isinstance(first_val, str):
                                    escaped_val = first_val.replace("'", "''")
                                    where_clause = f"{first_col} = '{escaped_val}'"
                                else:
                                    where_clause = f"{first_col} = {first_val}"
                                
                                # Prepare update data with auto-generated fields
                                update_data = {
                                    "business_reviewed_size": form_data.get("business_reviewed_size", ""),
                                    "business_reviewed_gender": form_data.get("business_reviewed_gender", "")
                                }
                                
                                # Add auto-generated fields if they exist in schema
                                if "reviewed_by" in st.session_state.table_schema:
                                    update_data["reviewed_by"] = get_current_user_email()
                                
                                if "business_reviewed_date" in st.session_state.table_schema:
                                    update_data["business_reviewed_date"] = get_manila_timestamp()
                                
                                update_record(TABLE_NAME, update_data, where_clause, conn)
                                st.success("✅ Record updated successfully!")
                                st.session_state.table_data = None  # Force refresh
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error updating record: {str(e)}")
        else:
            st.info("No records found in the table.")
    else:
        st.info("👆 Click 'Connect to Table' to start reviewing merchant data.")

with tab_view:
    st.subheader("📊 Table Data View with Inline Editing")
    
    if st.session_state.table_data is not None:
        # Get current table name
        TABLE_NAME = st.session_state.current_table_name
        
        # Display current table info
        st.info(f"📋 **Current Table:** `{TABLE_NAME}`")
        
        # Search functionality
        search_term = st.text_input("🔍 Search records:", placeholder="Enter search term...", key="search_table_view")
        
        display_data = st.session_state.table_data.copy()
        
        if search_term and search_term.strip():
            # Simple search across all columns
            search_lower = search_term.lower().strip()
            mask = display_data.astype(str).apply(
                lambda x: x.str.lower().str.contains(search_lower, na=False, regex=False)
            ).any(axis=1)
            display_data = display_data[mask]
            st.info(f"Found {len(display_data)} records matching '{search_term}'")
        
        st.info("ℹ️ **Inline Editing:** You can edit business_reviewed_size and business_reviewed_gender directly in the table below. Click 'Save Changes' when done.")
        
        # Configure column settings - make only review fields editable
        column_config = {}
        for col in display_data.columns:
            if col == "business_reviewed_size":
                column_config[col] = st.column_config.SelectboxColumn(
                    "Business Reviewed Size",
                    help="Select business size",
                    options=["", "MICRO", "SMALL", "MEDIUM", "LARGE"],
                    required=False
                )
            elif col == "business_reviewed_gender":
                column_config[col] = st.column_config.SelectboxColumn(
                    "Business Reviewed Gender",
                    help="Select gender",
                    options=["", "MALE", "FEMALE"],
                    required=False
                )
            else:
                # Make all other columns read-only
                column_config[col] = st.column_config.Column(
                    col,
                    disabled=True
                )
        
        # Use data_editor for inline editing
        edited_data = st.data_editor(
            display_data,
            use_container_width=True,
            hide_index=True,
            column_config=column_config,
            num_rows="fixed",
            key="data_editor"
        )
        
        # Save changes button
        if st.button("💾 Save All Changes", type="primary"):
            try:
                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                
                # Get auto-generated values once
                user_email = get_current_user_email()
                manila_timestamp = get_manila_timestamp()
                
                # Find rows that were modified
                changes_made = False
                for idx in display_data.index:
                    original_row = display_data.loc[idx]
                    edited_row = edited_data.loc[idx]
                    
                    # Check if review fields changed
                    size_changed = original_row.get("business_reviewed_size") != edited_row.get("business_reviewed_size")
                    gender_changed = original_row.get("business_reviewed_gender") != edited_row.get("business_reviewed_gender")
                    
                    if size_changed or gender_changed:
                        # Create WHERE clause using first column as identifier
                        first_col = list(st.session_state.table_schema.keys())[0]
                        first_val = original_row[first_col]
                        if isinstance(first_val, str):
                            escaped_val = first_val.replace("'", "''")
                            where_clause = f"{first_col} = '{escaped_val}'"
                        else:
                            where_clause = f"{first_col} = {first_val}"
                        
                        # Prepare update data with review fields
                        update_data = {
                            "business_reviewed_size": edited_row.get("business_reviewed_size", ""),
                            "business_reviewed_gender": edited_row.get("business_reviewed_gender", "")
                        }
                        
                        # Add auto-generated fields if they exist in schema
                        if "reviewed_by" in st.session_state.table_schema:
                            update_data["reviewed_by"] = user_email
                        
                        if "business_reviewed_date" in st.session_state.table_schema:
                            update_data["business_reviewed_date"] = manila_timestamp
                        
                        update_record(TABLE_NAME, update_data, where_clause, conn)
                        changes_made = True
                
                if changes_made:
                    st.success("✅ All changes saved successfully!")
                    st.session_state.table_data = None  # Force refresh
                    st.rerun()
                else:
                    st.info("ℹ️ No changes detected.")
                    
            except Exception as e:
                st.error(f"❌ Error saving changes: {str(e)}")
        
        # Table statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(st.session_state.table_data))
        with col2:
            st.metric("Displayed", len(display_data))
        with col3:
            st.metric("Columns", len(st.session_state.table_data.columns))
        with col4:
            memory_kb = st.session_state.table_data.memory_usage(deep=True).sum() / 1024
            st.metric("Memory Usage", f"{memory_kb:.1f} KB")
    else:
        st.info("Connect to the table in the Form Editor tab to view data here.")
