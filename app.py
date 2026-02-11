import streamlit as st
from view_groups import get_groups_for_user
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(layout="wide")
st.logo("assets/logo.svg")
st.title(":material/verified: Databricks Apps :material/fact_check:")

# Get current user email
def get_current_user_email() -> str:
    """Get the current user's Databricks email"""
    try:
        # Method 1: Try Streamlit context headers
        try:
            if hasattr(st, 'context') and hasattr(st.context, 'headers'):
                headers = st.context.headers
                if headers:
                    username = headers.get("X-Forwarded-Preferred-Username")
                    if username and '@' in username:
                        return username
        except Exception:
            pass
        
        # Method 2: Try Streamlit experimental user
        if hasattr(st, 'experimental_user') and st.experimental_user:
            user_email = st.experimental_user.get('email')
            if user_email and '@' in user_email:
                return user_email
        
        # Method 3: Try WorkspaceClient
        w = WorkspaceClient()
        current_user = w.current_user.me()
        
        if current_user.user_name and '@' in str(current_user.user_name):
            return current_user.user_name
        
        if hasattr(current_user, 'emails') and current_user.emails and len(current_user.emails) > 0:
            email_value = current_user.emails[0].value
            if email_value and '@' in email_value:
                return email_value
        
        return "unknown@databricks.com"
        
    except Exception as e:
        return "unknown@databricks.com"

# Get user email and load appropriate pages
current_user_email = get_current_user_email()
groups = get_groups_for_user(current_user_email)

pages = {
    group.get("title", ""): [
        st.Page(
            view.get("page"),
            title=view.get("label"),
            icon=view.get("icon"),
        )
        for view in group["views"]
    ]
    for group in groups
}

pg = st.navigation(pages)
pg.run()
