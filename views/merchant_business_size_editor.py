import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import pytz
import sys
import os

# Add config directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.user_roles import get_user_role, is_admin, is_maker, is_checker

# Pre-configured connection details
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"

# Available tables
AVAILABLE_TABLES = {
    # "Dev - Merchant Business Size": "dg_dev.sandbox.out_merchant_business_size_for_bank",
    "Prod Test - Merchant Business Size": "dg_prod.sandbox.out_merchant_business_size_for_bank_test"
}

# Dropdown values for review fields (no blank option)
BUSINESS_SIZE_OPTIONS = ["MICRO", "SMALL", "MEDIUM", "LARGE"]
GENDER_OPTIONS = ["MALE", "FEMALE"]

# Workflow statuses
STATUS_PENDING = "PENDING"
STATUS_APPROVED = "APPROVED"
STATUS_REJECTED = "REJECTED"

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
if 'user_role' not in st.session_state:
    st.session_state.user_role = "MAKER"  # Default role

@st.cache_resource(ttl="1h")
def get_connection(server_hostname: str, http_path: str):
    """Create connection to Databricks SQL warehouse"""
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: Config().authenticate,
    )

def get_current_user_email() -> str:
    """Get the current user's Databricks email from request headers"""
    try:
        # Method 1: Try to get from Streamlit request headers (Databricks Apps)
        # This is the correct way for Databricks Apps with OBO enabled
        if hasattr(st, 'context') and hasattr(st.context, 'headers'):
            headers = st.context.headers
            # X-Forwarded-Preferred-Username contains the actual user email
            username = headers.get("X-Forwarded-Preferred-Username")
            if username:
                return username
        
        # Method 2: Try SQL query to get current user
        try:
            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_user()")
                result = cursor.fetchone()
                if result and result[0]:
                    user_value = result[0]
                    if '@' in str(user_value):
                        return str(user_value)
        except Exception:
            pass
        
        # Method 3: Try WorkspaceClient
        w = WorkspaceClient()
        current_user = w.current_user.me()
        
        if current_user.user_name and '@' in str(current_user.user_name):
            return current_user.user_name
        
        if hasattr(current_user, 'emails') and current_user.emails and len(current_user.emails) > 0:
            email_value = current_user.emails[0].value
            if email_value and '@' in email_value:
                return email_value
        
        # If we only have an ID, return it
        user_id = str(current_user.id) if hasattr(current_user, 'id') and current_user.id else "unknown"
        return user_id
        
    except Exception as e:
        return "unknown@databricks.com"

def get_manila_timestamp() -> str:
    """Get current timestamp in Manila timezone"""
    manila_tz = pytz.timezone('Asia/Manila')
    manila_time = datetime.now(manila_tz)
    return manila_time.strftime('%Y-%m-%d %H:%M:%S')

def get_table_schema(table_name: str, conn) -> Dict[str, str]:
    """Get table schema information"""
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema_info = cursor.fetchall()
        return {row[0]: row[1] for row in schema_info}

def read_table(table_name: str, conn, limit: int = 1000, status_filter: str = None) -> pd.DataFrame:
    """Read table data with optional status filter"""
    with conn.cursor() as cursor:
        if status_filter:
            query = f"SELECT * FROM {table_name} WHERE review_status = '{status_filter}' LIMIT {limit}"
        else:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

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

def render_form_field(column_name: str, column_type: str, current_value: Any = None, key_suffix: str = "", disabled: bool = False):
    """Render appropriate form field based on column type"""
    if current_value is None:
        current_value = ""
    
    # Handle pandas NaN values
    if pd.isna(current_value):
        current_value = ""
    
    field_key = f"{column_name}_{key_suffix}" if key_suffix else column_name
    
    # Special handling for review fields with dropdowns
    if column_name in ["business_reviewed_size_pending", "business_reviewed_size"]:
        current_str = str(current_value) if current_value != "" and current_value is not None else ""
        try:
            default_index = BUSINESS_SIZE_OPTIONS.index(current_str) if current_str in BUSINESS_SIZE_OPTIONS else 0
        except ValueError:
            default_index = 0
        return st.selectbox(
            f"{column_name}",
            options=BUSINESS_SIZE_OPTIONS,
            index=default_index,
            key=field_key,
            help="Select business size: MICRO, SMALL, MEDIUM, or LARGE",
            disabled=disabled
        )
    elif column_name in ["business_reviewed_gender_pending", "business_reviewed_gender"]:
        current_str = str(current_value) if current_value != "" and current_value is not None else ""
        try:
            default_index = GENDER_OPTIONS.index(current_str) if current_str in GENDER_OPTIONS else 0
        except ValueError:
            default_index = 0
        return st.selectbox(
            f"{column_name}",
            options=GENDER_OPTIONS,
            index=default_index,
            key=field_key,
            help="Select gender: MALE or FEMALE",
            disabled=disabled
        )
    else:
        # Default to text input for other types
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            key=field_key,
            disabled=disabled
        )

# Page header and user info
st.header(body="Merchant Business Size and Gender Review", divider=True)
st.subheader("Maker-Checker Workflow")

# Display current user and determine role
current_user = get_current_user_email()
user_base_role = get_user_role(current_user)

# Display user info with role
if '@' in current_user:
    if user_base_role == "UNAUTHORIZED":
        st.error(f"🚫 **Access Denied:** {current_user} is not authorized to use this application.")
        st.info("Please contact your administrator to request access.")
        st.stop()
    else:
        col_user, col_role_badge = st.columns([3, 1])
        with col_user:
            st.info(f"👤 **Logged in as:** {current_user}")
        with col_role_badge:
            if user_base_role == "ADMIN":
                st.success(f"🔑 **Role:** {user_base_role}")
            elif user_base_role == "MAKER":
                st.info(f"📝 **Role:** {user_base_role}")
            elif user_base_role == "CHECKER":
                st.warning(f"✅ **Role:** {user_base_role}")
else:
    st.warning(f"⚠️ **User ID:** {current_user}")
    user_base_role = "ADMIN"  # Fallback for development

# Role selector (only for admins)
col_role, col_table = st.columns(2)
with col_role:
    if user_base_role == "ADMIN":
        # Admins can switch roles
        user_role = st.selectbox(
            "🎭 Switch Role (Admin Only):",
            options=["MAKER", "CHECKER"],
            index=0 if st.session_state.user_role == "MAKER" else 1,
            help="As an admin, you can switch between roles to test the workflow"
        )
        st.session_state.user_role = user_role
    else:
        # Non-admins see their assigned role (read-only)
        st.session_state.user_role = user_base_role
        st.text_input(
            "🎭 Your Role:",
            value=user_base_role,
            disabled=True,
            help="Your role is determined by your email address"
        )
        user_role = user_base_role

with col_table:
    selected_table_name = st.selectbox(
        "📊 Select Table:",
        options=list(AVAILABLE_TABLES.keys()),
        index=list(AVAILABLE_TABLES.keys()).index(st.session_state.selected_table)
    )
    
    if selected_table_name != st.session_state.selected_table:
        st.session_state.selected_table = selected_table_name
        st.session_state.current_table_name = AVAILABLE_TABLES[selected_table_name]
        st.session_state.connection_established = False
        st.session_state.table_data = None
        st.session_state.table_schema = None

TABLE_NAME = st.session_state.current_table_name

# Connection section
st.info(f"🔗 **Table:** `{TABLE_NAME}`")

if st.button("🔌 Connect to Table", type="primary"):
    try:
        with st.spinner("Connecting to Databricks..."):
            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
            st.session_state.table_data = read_table(TABLE_NAME, conn)
            st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
            st.session_state.connection_established = True
        st.success("✅ Successfully connected!")
        st.rerun()
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)}")
        st.session_state.connection_established = False

# Main workflow interface
if st.session_state.connection_established and st.session_state.table_data is not None:
    
    if user_role == "MAKER":
        # ============ MAKER INTERFACE ============
        st.markdown("---")
        st.subheader("📝 Maker: Submit Reviews")
        
        tab_submit, tab_my_submissions = st.tabs(["Submit New Review", "My Submissions"])
        
        with tab_submit:
            st.write("**Inline Table Editor:** Edit business size and gender directly in the table, then click 'Submit for Approval'")
            
            if st.button("🔄 Refresh Records", key="maker_refresh"):
                st.session_state.table_data = None
                st.rerun()
            
            if len(st.session_state.table_data) > 0:
                # Filter to show only records that are not pending or can be resubmitted
                display_data = st.session_state.table_data.copy()
                
                # Configure column settings for inline editing
                column_config = {}
                for col in display_data.columns:
                    if col == "business_reviewed_size_pending":
                        column_config[col] = st.column_config.SelectboxColumn(
                            "Business Size (Pending) 🟣",
                            help="Select business size",
                            options=BUSINESS_SIZE_OPTIONS,
                            required=True
                        )
                    elif col == "business_reviewed_gender_pending":
                        column_config[col] = st.column_config.SelectboxColumn(
                            "Gender (Pending) 🟣",
                            help="Select gender",
                            options=GENDER_OPTIONS,
                            required=True
                        )
                    elif col in ["cpm_id", "business_name", "trade_name"]:
                        # Freeze these columns
                        column_config[col] = st.column_config.Column(
                            col.replace("_", " ").title(),
                            disabled=True,
                            pinned=True
                        )
                    elif col in ["review_status", "reviewed_by_maker", "reviewed_date_maker", 
                               "reviewed_by_checker", "reviewed_date_checker", "checker_comments"]:
                        # Show these columns but make them read-only
                        column_config[col] = st.column_config.Column(
                            col.replace("_", " ").title(),
                            disabled=True
                        )
                    else:
                        # Make all other columns read-only
                        column_config[col] = st.column_config.Column(
                            col,
                            disabled=True
                        )
                
                # Note: Streamlit data_editor doesn't support custom column colors
                # The columns are configured but colors must be applied via Streamlit theme
                
                # Use data_editor for inline editing
                edited_data = st.data_editor(
                    display_data,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config,
                    num_rows="fixed",
                    key="maker_data_editor"
                )
                
                # Submit button
                if st.button("📤 Submit Selected Records for Approval", type="primary", key="maker_submit"):
                    try:
                        conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                        submitted_count = 0
                        
                        for idx in display_data.index:
                            original_row = display_data.loc[idx]
                            edited_row = edited_data.loc[idx]
                            
                            # Check if review fields were filled or changed
                            size_val = edited_row.get("business_reviewed_size_pending")
                            gender_val = edited_row.get("business_reviewed_gender_pending")
                            
                            # Only submit if both fields are filled
                            if pd.notna(size_val) and pd.notna(gender_val) and size_val != "" and gender_val != "":
                                # Check if values changed or are new
                                orig_size = original_row.get("business_reviewed_size_pending")
                                orig_gender = original_row.get("business_reviewed_gender_pending")
                                
                                if (pd.isna(orig_size) or orig_size != size_val or 
                                    pd.isna(orig_gender) or orig_gender != gender_val):
                                    
                                    # Create WHERE clause
                                    first_col = list(st.session_state.table_schema.keys())[0]
                                    first_val = original_row[first_col]
                                    
                                    if isinstance(first_val, str):
                                        escaped_val = first_val.replace("'", "''")
                                        where_clause = f"{first_col} = '{escaped_val}'"
                                    else:
                                        where_clause = f"{first_col} = {first_val}"
                                    
                                    update_data = {
                                        "business_reviewed_size_pending": size_val,
                                        "business_reviewed_gender_pending": gender_val,
                                        "review_status": STATUS_PENDING,
                                        "reviewed_by_maker": current_user,
                                        "reviewed_date_maker": get_manila_timestamp()
                                    }
                                    
                                    update_record(TABLE_NAME, update_data, where_clause, conn)
                                    submitted_count += 1
                        
                        if submitted_count > 0:
                            st.success(f"✅ Submitted {submitted_count} record(s) for approval!")
                            st.session_state.table_data = None
                            st.rerun()
                        else:
                            st.info("ℹ️ No changes detected or no records with both fields filled.")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
            else:
                st.info("No records found")
        
        with tab_my_submissions:
            st.write("View your submitted reviews and their status")
            
            if st.button("🔄 Refresh My Submissions"):
                st.session_state.table_data = None
                st.rerun()
            
            # Filter for current user's submissions
            my_submissions = st.session_state.table_data[
                st.session_state.table_data['reviewed_by_maker'] == current_user
            ]
            
            if len(my_submissions) > 0:
                # Status filter
                status_filter = st.multiselect(
                    "Filter by status:",
                    options=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED],
                    default=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED]
                )
                
                filtered_data = my_submissions[my_submissions['review_status'].isin(status_filter)]
                
                # Configure column settings with frozen columns and visual indicators
                column_config = {}
                for col in filtered_data.columns:
                    if col in ["cpm_id", "business_name", "trade_name"]:
                        # Freeze these columns
                        column_config[col] = st.column_config.Column(
                            col.replace("_", " ").title(),
                            pinned=True
                        )
                    elif col == "business_reviewed_size_pending":
                        column_config[col] = st.column_config.Column(
                            "Business Size (Pending) 🟣"
                        )
                    elif col == "business_reviewed_gender_pending":
                        column_config[col] = st.column_config.Column(
                            "Gender (Pending) 🟣"
                        )
                
                # Remove CSS styling as it doesn't work with st.dataframe
                
                # Select relevant columns to display
                display_columns = [col for col in filtered_data.columns 
                    if col in ['cpm_id', 'business_name', 'trade_name',
                             'business_reviewed_size_pending', 'business_reviewed_gender_pending',
                             'review_status', 'reviewed_date_maker', 'reviewed_by_checker',
                             'reviewed_date_checker', 'checker_comments']]
                
                st.dataframe(
                    filtered_data[display_columns],
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config
                )
                
                st.metric("Total Submissions", len(my_submissions))
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Pending", len(my_submissions[my_submissions['review_status'] == STATUS_PENDING]))
                with col2:
                    st.metric("Approved", len(my_submissions[my_submissions['review_status'] == STATUS_APPROVED]))
                with col3:
                    st.metric("Rejected", len(my_submissions[my_submissions['review_status'] == STATUS_REJECTED]))
            else:
                st.info("You haven't submitted any reviews yet")
    
    else:
        # ============ CHECKER INTERFACE ============
        st.markdown("---")
        st.subheader("✅ Checker: Review & Approve")
        
        tab_pending, tab_all = st.tabs(["Pending Reviews", "All Reviews"])
        
        with tab_pending:
            if st.button("🔄 Refresh Pending", key="checker_refresh"):
                st.session_state.table_data = None
                st.rerun()
            
            # Filter pending reviews
            pending_reviews = st.session_state.table_data[
                st.session_state.table_data['review_status'] == STATUS_PENDING
            ].copy()  # Make a copy to avoid modifying original
            
            if len(pending_reviews) > 0:
                st.info(f"📋 {len(pending_reviews)} pending review(s)")
                st.write("**Inline Table Editor:** Review and edit values directly in the table, then Approve or Reject")
                
                # Pre-populate the final columns with pending values for editing
                # This allows the checker to see and modify the maker's proposed values
                if 'business_reviewed_size' in pending_reviews.columns:
                    pending_reviews['business_reviewed_size'] = pending_reviews.apply(
                        lambda row: row['business_reviewed_size'] if pd.notna(row['business_reviewed_size']) and row['business_reviewed_size'] != '' 
                        else row['business_reviewed_size_pending'], axis=1
                    )
                if 'business_reviewed_gender' in pending_reviews.columns:
                    pending_reviews['business_reviewed_gender'] = pending_reviews.apply(
                        lambda row: row['business_reviewed_gender'] if pd.notna(row['business_reviewed_gender']) and row['business_reviewed_gender'] != '' 
                        else row['business_reviewed_gender_pending'], axis=1
                    )
                
                # Configure column settings for inline editing
                column_config = {}
                for col in pending_reviews.columns:
                    if col == "business_reviewed_size":
                        column_config[col] = st.column_config.SelectboxColumn(
                            "Business Size (Final) 🟢",
                            help="Edit if needed before approving",
                            options=BUSINESS_SIZE_OPTIONS,
                            required=True
                        )
                    elif col == "business_reviewed_gender":
                        column_config[col] = st.column_config.SelectboxColumn(
                            "Gender (Final) 🟢",
                            help="Edit if needed before approving",
                            options=GENDER_OPTIONS,
                            required=True
                        )
                    elif col in ["cpm_id", "business_name", "trade_name"]:
                        # Freeze these columns
                        column_config[col] = st.column_config.Column(
                            col.replace("_", " ").title(),
                            disabled=True,
                            pinned=True
                        )
                    elif col == "business_reviewed_size_pending":
                        column_config[col] = st.column_config.Column(
                            "Business Size (Pending) 🟣",
                            disabled=True
                        )
                    elif col == "business_reviewed_gender_pending":
                        column_config[col] = st.column_config.Column(
                            "Gender (Pending) 🟣",
                            disabled=True
                        )
                    elif col in ["reviewed_by_maker", "reviewed_date_maker", "review_status"]:
                        # Show these columns as read-only
                        column_config[col] = st.column_config.Column(
                            col.replace("_", " ").title(),
                            disabled=True
                        )
                    elif col not in ["reviewed_by_checker", "reviewed_date_checker", "checker_comments"]:
                        # Make other columns read-only
                        column_config[col] = st.column_config.Column(
                            col,
                            disabled=True
                        )
                
                # Note: Streamlit data_editor doesn't support custom column background colors
                # Using emoji indicators instead: 🟣 for pending, 🟢 for final
                
                # Use data_editor for inline editing
                edited_data = st.data_editor(
                    pending_reviews,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config,
                    num_rows="fixed",
                    key="checker_data_editor"
                )
                
                # Add comments field
                checker_comments = st.text_area(
                    "Comments (required for rejection, optional for approval):",
                    key="checker_comments_bulk"
                )
                
                # Action buttons
                col_approve, col_reject = st.columns(2)
                with col_approve:
                    if st.button("✅ Approve Selected Records", type="primary", key="checker_approve"):
                        try:
                            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                            approved_count = 0
                            
                            for idx in pending_reviews.index:
                                original_row = pending_reviews.loc[idx]
                                edited_row = edited_data.loc[idx]
                                
                                # Get the final values to approve
                                # Priority: 1) Checker's edits, 2) Maker's pending values
                                size_val = edited_row.get("business_reviewed_size")
                                gender_val = edited_row.get("business_reviewed_gender")
                                
                                # If checker didn't edit (columns are empty/null), use pending values
                                if pd.isna(size_val) or size_val == "":
                                    size_val = original_row.get("business_reviewed_size_pending")
                                if pd.isna(gender_val) or gender_val == "":
                                    gender_val = original_row.get("business_reviewed_gender_pending")
                                
                                # Create WHERE clause
                                first_col = list(st.session_state.table_schema.keys())[0]
                                first_val = original_row[first_col]
                                
                                if isinstance(first_val, str):
                                    escaped_val = first_val.replace("'", "''")
                                    where_clause = f"{first_col} = '{escaped_val}'"
                                else:
                                    where_clause = f"{first_col} = {first_val}"
                                
                                update_data = {
                                    "business_reviewed_size": size_val,
                                    "business_reviewed_gender": gender_val,
                                    "review_status": STATUS_APPROVED,
                                    "reviewed_by_checker": current_user,
                                    "reviewed_date_checker": get_manila_timestamp(),
                                    "checker_comments": checker_comments if checker_comments else ""
                                }
                                
                                update_record(TABLE_NAME, update_data, where_clause, conn)
                                approved_count += 1
                            
                            st.success(f"✅ Approved {approved_count} record(s)!")
                            st.session_state.table_data = None
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                
                with col_reject:
                    if st.button("❌ Reject Selected Records", type="secondary", key="checker_reject"):
                        if not checker_comments:
                            st.error("❌ Please provide comments when rejecting")
                        else:
                            try:
                                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                rejected_count = 0
                                
                                for idx in pending_reviews.index:
                                    edited_row = edited_data.loc[idx]
                                    
                                    # Create WHERE clause
                                    first_col = list(st.session_state.table_schema.keys())[0]
                                    first_val = edited_row[first_col]
                                    
                                    if isinstance(first_val, str):
                                        escaped_val = first_val.replace("'", "''")
                                        where_clause = f"{first_col} = '{escaped_val}'"
                                    else:
                                        where_clause = f"{first_col} = {first_val}"
                                    
                                    update_data = {
                                        "review_status": STATUS_REJECTED,
                                        "reviewed_by_checker": current_user,
                                        "reviewed_date_checker": get_manila_timestamp(),
                                        "checker_comments": checker_comments
                                    }
                                    
                                    update_record(TABLE_NAME, update_data, where_clause, conn)
                                    rejected_count += 1
                                
                                st.success(f"✅ Rejected {rejected_count} record(s)!")
                                st.session_state.table_data = None
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
            else:
                st.success("🎉 No pending reviews!")
        
        with tab_all:
            st.write("View all reviews with their status")
            
            # Status filter
            status_filter = st.multiselect(
                "Filter by status:",
                options=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED],
                default=[STATUS_PENDING, STATUS_APPROVED, STATUS_REJECTED],
                key="all_status_filter"
            )
            
            filtered_data = st.session_state.table_data[
                st.session_state.table_data['review_status'].isin(status_filter)
            ]
            
            if len(filtered_data) > 0:
                # Configure column settings with frozen columns and visual indicators
                column_config = {}
                for col in filtered_data.columns:
                    if col in ["cpm_id", "business_name", "trade_name"]:
                        # Freeze these columns
                        column_config[col] = st.column_config.Column(
                            col.replace("_", " ").title(),
                            pinned=True
                        )
                    elif col == "business_reviewed_size_pending":
                        column_config[col] = st.column_config.Column(
                            "Business Size (Pending) 🟣"
                        )
                    elif col == "business_reviewed_gender_pending":
                        column_config[col] = st.column_config.Column(
                            "Gender (Pending) 🟣"
                        )
                    elif col == "business_reviewed_size":
                        column_config[col] = st.column_config.Column(
                            "Business Size (Final) 🟢"
                        )
                    elif col == "business_reviewed_gender":
                        column_config[col] = st.column_config.Column(
                            "Gender (Final) 🟢"
                        )
                
                # Note: Streamlit dataframe doesn't support custom column background colors
                # Using emoji indicators instead: 🟣 for pending, 🟢 for final
                
                # Select relevant columns to display
                display_columns = [col for col in filtered_data.columns 
                    if col in ['cpm_id', 'business_name', 'trade_name',
                             'business_reviewed_size_pending', 'business_reviewed_gender_pending',
                             'business_reviewed_size', 'business_reviewed_gender',
                             'review_status', 'reviewed_by_maker', 'reviewed_date_maker',
                             'reviewed_by_checker', 'reviewed_date_checker', 'checker_comments']]
                
                st.dataframe(
                    filtered_data[display_columns],
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config
                )
                
                # Statistics
                st.markdown("### 📊 Statistics")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total", len(st.session_state.table_data))
                with col2:
                    st.metric("Pending", len(st.session_state.table_data[st.session_state.table_data['review_status'] == STATUS_PENDING]))
                with col3:
                    st.metric("Approved", len(st.session_state.table_data[st.session_state.table_data['review_status'] == STATUS_APPROVED]))
                with col4:
                    st.metric("Rejected", len(st.session_state.table_data[st.session_state.table_data['review_status'] == STATUS_REJECTED]))
            else:
                st.info("No reviews found")

else:
    st.info("👆 Click 'Connect to Table' to start")
