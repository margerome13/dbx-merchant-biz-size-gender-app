from config.user_roles import is_admin

def get_groups_for_user(user_email: str):
    """
    Return page groups based on user role.
    Admin users see both pages, non-admin users see only CSV Upload.
    """
    views = []
    
    # Admin users see both pages
    if is_admin(user_email):
        views.append({
            "label": "Merchant Business Size & Gender Review",
            "help": "Maker-checker workflow for reviewing merchant business size and gender classifications.",
            "page": "views/merchant_business_size_editor.py",
            "icon": ":material/fact_check:",
        })
    
    # All users see CSV Upload page
    views.append({
        "label": "CSV Upload to Table",
        "help": "Upload CSV files and create or update Databricks Delta tables for bulk data import.",
        "page": "views/csv_upload_to_table.py",
        "icon": ":material/upload_file:",
    })
    
    return [{"views": views}]

# Default groups (for backward compatibility)
groups = [
    {
        "views": [
            {
                "label": "CSV Upload to Table",
                "help": "Upload CSV files and create or update Databricks Delta tables for bulk data import.",
                "page": "views/csv_upload_to_table.py",
                "icon": ":material/upload_file:",
            },
        ],
    },
]
