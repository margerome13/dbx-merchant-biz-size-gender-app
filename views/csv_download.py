import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from datetime import datetime
import pytz

from config.download_queries import DOWNLOAD_QUERIES

# Connection details
cfg = Config()
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"

def get_user_token() -> str | None:
    """Get the on-behalf-of user token from Streamlit context headers.
    Returns None if OBO is not enabled for this app."""
    try:
        headers = st.context.headers
        token = headers.get("x-forwarded-access-token")
        if token:
            return token
    except Exception:
        pass
    return None

@st.cache_resource(ttl=300, show_spinner=True)
def get_obo_connection(http_path: str, user_token: str):
    """Create connection using the user's OBO token."""
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        access_token=user_token,
    )

@st.cache_resource(ttl="1h")
def get_sp_connection(http_path: str):
    """Fallback connection using service principal credentials."""
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def get_current_user_email() -> str:
    """Get the current user's Databricks email"""
    try:
        try:
            if hasattr(st, 'context') and hasattr(st.context, 'headers'):
                headers = st.context.headers
                if headers:
                    username = headers.get("X-Forwarded-Preferred-Username")
                    if username and '@' in username:
                        return username
        except Exception:
            pass
        if hasattr(st, 'experimental_user') and st.experimental_user:
            user_email = st.experimental_user.get('email')
            if user_email and '@' in user_email:
                return user_email
        w_client = WorkspaceClient()
        current_user = w_client.current_user.me()
        if current_user.user_name and '@' in str(current_user.user_name):
            return current_user.user_name
        return "unknown@databricks.com"
    except Exception:
        return "unknown@databricks.com"

def get_manila_timestamp() -> str:
    manila_tz = pytz.timezone('Asia/Manila')
    manila_time = datetime.now(manila_tz)
    return manila_time.strftime('%Y-%m-%d_%H-%M-%S')

# Page header
st.header(body="CSV Download from Databricks", divider=True)
st.subheader("On-Demand Data Export")

# Display current user
current_user = get_current_user_email()
if '@' in current_user:
    st.info(f"👤 **Logged in as:** {current_user}")

# Query selection
query_keys = list(DOWNLOAD_QUERIES.keys())
query_labels = [DOWNLOAD_QUERIES[k]["label"] for k in query_keys]

selected_label = st.selectbox(
    "Select Data Export Query:",
    options=query_labels,
    help="Choose which data export to run"
)

# Get selected query config
selected_key = query_keys[query_labels.index(selected_label)]
query_config = DOWNLOAD_QUERIES[selected_key]

# Show query details
st.markdown(f"**Description:** {query_config['description']}")
st.markdown(f"**Target Table:** `{query_config['target_table']}`")

with st.expander("📝 View SQL Query"):
    st.code(query_config["sql"], language="sql")

st.markdown("---")

# Initialize session state for results
if 'download_df' not in st.session_state:
    st.session_state.download_df = None
if 'download_query_key' not in st.session_state:
    st.session_state.download_query_key = None

# Execute query button
if st.button("🚀 Execute Query & Generate Table", type="primary", use_container_width=True):
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()

        # Step 1: Connect (OBO if available, otherwise service principal)
        user_token = get_user_token()
        if user_token:
            status_text.info("🔌 Connecting as current user (on-behalf-of)...")
            progress_bar.progress(10)
            conn = get_obo_connection(HTTP_PATH, user_token)
        else:
            status_text.info("🔌 Connecting to SQL warehouse...")
            progress_bar.progress(10)
            conn = get_sp_connection(HTTP_PATH)

        # Step 2: Execute CREATE TABLE query
        status_text.info("⚙️ Executing query (this may take a few minutes)...")
        progress_bar.progress(30)
        with conn.cursor() as cursor:
            cursor.execute(query_config["sql"])

        # Step 3: Read data from the created table
        status_text.info("📊 Reading data from table...")
        progress_bar.progress(70)
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {query_config['target_table']}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        # Step 4: Convert to DataFrame
        status_text.info("📋 Preparing data...")
        progress_bar.progress(90)
        df = pd.DataFrame(rows, columns=columns)

        st.session_state.download_df = df
        st.session_state.download_query_key = selected_key

        progress_bar.progress(100)
        status_text.success("✅ Query executed successfully!")

    except Exception as e:
        st.error(f"❌ Error executing query: {str(e)}")
        st.exception(e)

# Display results and download button if data is available
if st.session_state.download_df is not None and st.session_state.download_query_key == selected_key:
    df = st.session_state.download_df

    st.markdown("---")
    st.subheader("📊 Results")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Rows", len(df))
    with col2:
        st.metric("Total Columns", len(df.columns))

    # Preview data
    with st.expander("📋 Data Preview (First 20 rows)", expanded=True):
        st.dataframe(df.head(20), use_container_width=True)

    # Download button
    st.markdown("---")
    timestamp = get_manila_timestamp()
    file_name = f"{selected_key}_{timestamp}.csv"

    csv_data = df.to_csv(index=False)

    st.download_button(
        label="📥 Download as CSV",
        data=csv_data,
        file_name=file_name,
        mime="text/csv",
        type="primary",
        use_container_width=True
    )

    st.caption(f"File will be named: `{file_name}`")

# Footer
st.markdown("---")
st.caption("CSV Download Feature | Databricks Apps")
