# Merchant Business Size and Gender Review App

A Databricks-powered Streamlit application implementing a maker-checker workflow for reviewing and validating merchant business size and gender classifications.

## Overview

This application provides a dual-role interface for data governance teams to review and approve merchant business classifications. It implements a robust maker-checker workflow to ensure data quality and accuracy before finalizing business size and gender attributes.

## Key Features

### 🎭 Dual-Role System
- **Maker Role**: Submit reviews for merchant business size and gender classifications
- **Checker Role**: Review, approve, or reject submissions with optional edits and comments

### 🔄 Maker-Checker Workflow
The app implements a complete approval workflow with three statuses:
- **PENDING**: Awaiting checker review
- **APPROVED**: Validated and finalized by checker
- **REJECTED**: Returned to maker with feedback

### 📤 CSV Bulk Upload
- **Upload CSV files** to create or update Delta tables
- **Three upload modes**: Create new, Append, or Overwrite
- **Automatic backup** to Unity Catalog Volume
- **Data preview** and validation before upload
- **Metadata tracking** with upload timestamp and user

### 📊 Multi-Environment Support
- **Dev Environment**: `dg_dev.sandbox.out_merchant_business_size_for_bank`
- **Prod Test Environment**: `dg_prod.sandbox.out_merchant_business_size_for_bank_test`

### 🔐 User Authentication & Tracking
- Automatic user identification via Databricks authentication
- Full audit trail with timestamps (Manila timezone)
- Track who made changes and when

## Workflow

### Maker Workflow
1. **Connect** to the selected table
2. **Select** a merchant record to review
3. **Fill in** business size (MICRO, SMALL, MEDIUM, LARGE) and gender (MALE, FEMALE)
4. **Submit** for checker approval
5. **Track** submission status in "My Submissions" tab

### Checker Workflow
1. **Review** pending submissions from makers
2. **Verify** or **edit** the proposed classifications
3. **Approve** with optional comments or **Reject** with required feedback
4. **Monitor** all reviews and statistics

## Functionalities

### CSV Bulk Upload
- **Upload CSV Files**: Drag and drop or select CSV files for bulk data import
- **Three Upload Modes**:
  - Create New Table: Initialize new Delta tables from CSV
  - Append to Existing: Add new records to existing tables
  - Overwrite Existing: Replace all data in existing tables
- **Data Preview**: View first 10 rows and column information before upload
- **Automatic Backup**: CSV files stored in Unity Catalog Volume for audit trail
- **Metadata Tracking**: Optional columns for upload timestamp and user
- **Type Inference**: Automatic SQL type detection from CSV data
- **Progress Tracking**: Real-time upload progress with status updates

### For Makers
- **Submit New Review**: Select records and propose business size and gender classifications
- **My Submissions**: View all your submissions with status tracking
- **Status Filtering**: Filter by PENDING, APPROVED, or REJECTED
- **Resubmission**: Resubmit rejected records with corrections

### For Checkers
- **Pending Reviews**: Queue of submissions awaiting approval
- **Edit Before Approval**: Modify classifications if needed before approving
- **Rejection with Feedback**: Provide comments when rejecting submissions
- **All Reviews**: Comprehensive view of all reviews with statistics
- **Dashboard Metrics**: Real-time counts of pending, approved, and rejected reviews

### Data Management
- **Real-time Updates**: Direct connection to Databricks SQL warehouse
- **Audit Trail**: Complete tracking of maker and checker actions
- **Timestamp Tracking**: Manila timezone timestamps for all actions
- **Comments System**: Optional comments for approvals, required for rejections

## Technical Stack

- **Frontend**: Streamlit 1.51.0
- **Database**: Databricks SQL Warehouse
- **Authentication**: Databricks SDK
- **Data Processing**: Pandas 2.2.3
- **Timezone**: PyTZ (Asia/Manila)

## Installation

### Prerequisites
- Python 3.8+
- Databricks workspace access
- SQL warehouse access

### Setup

1. Clone the repository:
```bash
git clone https://github.com/margerome13/dbx-merchant-biz-size-gender-app.git
cd dbx-merchant-biz-size-gender-app
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure Databricks connection:
   - Update `DATABRICKS_HOST` in `views/merchant_business_size_editor.py`
   - Update `HTTP_PATH` with your SQL warehouse path

4. Run the application:
```bash
streamlit run app.py
```

## Configuration

### Database Tables
The app supports multiple tables configured in `AVAILABLE_TABLES`:
```python
AVAILABLE_TABLES = {
    "Dev - Merchant Business Size": "dg_dev.sandbox.out_merchant_business_size_for_bank",
    "Prod Test - Merchant Business Size": "dg_prod.sandbox.out_merchant_business_size_for_bank_test"
}
```

### Business Size Options
- MICRO
- SMALL
- MEDIUM
- LARGE

### Gender Options
- MALE
- FEMALE

## Database Schema Requirements

The target table must include these columns:
- `business_reviewed_size_pending` - Maker's proposed business size
- `business_reviewed_gender_pending` - Maker's proposed gender
- `business_reviewed_size` - Checker-approved business size
- `business_reviewed_gender` - Checker-approved gender
- `review_status` - Workflow status (PENDING/APPROVED/REJECTED)
- `reviewed_by_maker` - Maker's email
- `reviewed_date_maker` - Maker's submission timestamp
- `reviewed_by_checker` - Checker's email
- `reviewed_date_checker` - Checker's review timestamp
- `checker_comments` - Checker's feedback

## Usage

### CSV Bulk Upload
1. Navigate to "CSV Upload to Table" in the sidebar
2. Upload your CSV file (max recommended: 100MB)
3. Preview the data and column information
4. Specify target table name (catalog.schema.table_name)
5. Choose upload mode (Create/Append/Overwrite)
6. Optionally enable metadata columns
7. Click "Upload and Create/Update Table"
8. Verify success and check backup location

### As a Maker
1. Select "MAKER" role from the dropdown
2. Choose your target table
3. Click "Connect to Table"
4. Navigate to "Submit New Review" tab
5. Select a record from the dropdown
6. Fill in business size and gender
7. Click "Submit for Approval"
8. Monitor status in "My Submissions" tab

### As a Checker
1. Select "CHECKER" role from the dropdown
2. Choose your target table
3. Click "Connect to Table"
4. Navigate to "Pending Reviews" tab
5. Select a pending review
6. Review the maker's submission
7. Edit if necessary
8. Approve or reject with comments
9. View statistics in "All Reviews" tab

## Security & Compliance

- User authentication via Databricks
- Role-based access control (Maker/Checker)
- Complete audit trail for all actions
- Timestamp tracking in Manila timezone
- SQL injection protection with parameterized queries

## Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is proprietary and confidential.

## Support

For issues or questions, please contact the data governance team.

## Acknowledgments

Built with Databricks and Streamlit for efficient data governance workflows.
