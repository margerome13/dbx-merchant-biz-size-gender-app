groups = [
    {
        "views": [
            {
                "label": "Merchant Business Size & Gender Review",
                "help": "Maker-checker workflow for reviewing merchant business size and gender classifications.",
                "page": "views/merchant_business_size_editor.py",
                "icon": ":material/fact_check:",
            },
            {
                "label": "CSV Upload to Table",
                "help": "Upload CSV files and create or update Databricks Delta tables for bulk data import.",
                "page": "views/csv_upload_to_table.py",
                "icon": ":material/upload_file:",
            },
            {
                "label": "DQ MDAR Masterfile Editor",
                "help": "Pre-configured form editor for the DQ MDAR Inventory Masterfile table.",
                "page": "views/dq_mdar_masterfile_editor.py",
                "icon": ":material/folder_managed:",
            },
        ],
    },
]
