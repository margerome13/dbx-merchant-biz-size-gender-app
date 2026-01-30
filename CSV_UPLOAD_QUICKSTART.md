# CSV Upload Feature - Quick Start Guide

## What's New?

Your Merchant Business Size and Gender Review app now includes a **CSV Bulk Upload** feature that allows you to:
- Upload hundreds or thousands of merchant records at once
- Create new Delta tables or update existing ones
- Automatically backup CSV files to Unity Catalog Volumes
- Track who uploaded what and when

## Quick Start

### 1. Access the Feature
- Run your Streamlit app: `streamlit run app.py`
- Look for **"CSV Upload to Table"** in the sidebar navigation
- Click to open the upload interface

### 2. Prepare Your CSV
Your CSV should have columns matching your target table. Example:

```csv
merchant_id,merchant_name,business_reviewed_size_pending,business_reviewed_gender_pending,revenue,location
M001,ABC Trading,SMALL,MALE,50000,Manila
M002,XYZ Enterprises,MEDIUM,FEMALE,150000,Cebu
```

**Important columns for maker-checker workflow:**
- `business_reviewed_size_pending` - Business size (MICRO/SMALL/MEDIUM/LARGE)
- `business_reviewed_gender_pending` - Gender (MALE/FEMALE)
- `review_status` - Leave empty or set to NULL for new records

### 3. Upload Process

**Step 1: Upload CSV**
- Click "Choose a CSV file"
- Select your file
- Preview appears automatically

**Step 2: Configure Target**
- Enter table name: `dg_dev.sandbox.out_merchant_business_size_for_bank`
- Choose mode:
  - **Create New Table** - First time upload
  - **Append to Existing** - Add more records
  - **Overwrite Existing** - Replace all data

**Step 3: Optional Settings**
- Enable "Add metadata columns" to track:
  - `upload_timestamp` - When uploaded
  - `uploaded_by` - Who uploaded

**Step 4: Upload**
- Click "🚀 Upload and Create/Update Table"
- Wait for progress bar to complete
- Verify success message

### 4. Verify Upload

After successful upload:
1. Go to "Merchant Business Size & Gender Review" page
2. Click "Connect to Table"
3. Your uploaded records should appear
4. Makers can now review them

## Sample CSV File

A sample file `sample_merchant_data.csv` is included in your project with 10 test records.

## Upload Modes Explained

### Create New Table
```
Use when: First time uploading to a new table
Result: Creates table structure + inserts data
Safety: Fails if table already exists
```

### Append to Existing
```
Use when: Adding new records to existing table
Result: Keeps old data + adds new rows
Safety: Requires table to exist
```

### Overwrite Existing
```
Use when: Replacing all data in table
Result: Deletes old data + inserts new data
Safety: Use with caution! Old data is lost
```

## Common Workflows

### Workflow 1: Initial Bulk Load
1. Prepare CSV with merchant data
2. Use "Create New Table" mode
3. Upload to dev table first
4. Verify data looks correct
5. Makers review records
6. Checkers approve

### Workflow 2: Monthly Updates
1. Export new merchants to CSV
2. Use "Append to Existing" mode
3. Upload to production table
4. New records flow through maker-checker

### Workflow 3: Data Refresh
1. Get updated full dataset
2. Use "Overwrite Existing" mode
3. Replace all data at once
4. Re-review if needed

## Integration with Maker-Checker

The CSV upload integrates seamlessly:

```
CSV Upload → Delta Table → Maker Reviews → Checker Approves
     ↓
  (Backup in Volume for audit)
```

**After upload:**
- Records appear in maker interface
- Makers fill in or verify business size/gender
- Submit for checker approval
- Checkers approve/reject as normal

## Troubleshooting

### Error: "Permission denied"
**Solution:** Contact your Databricks admin to grant:
- `WRITE VOLUME` on `dg_dev.sandbox.csv_uploads`
- `CREATE TABLE` on target schema
- `CAN USE` on SQL warehouse

### Error: "Table already exists"
**Solution:** Change mode to "Append" or "Overwrite"

### Error: "Column mismatch"
**Solution:** Ensure CSV columns match table structure exactly

### Upload is slow
**Solution:** 
- Split large files into smaller chunks
- Upload during off-peak hours
- Consider using Databricks file upload for very large files

## Best Practices

1. **Test First**
   - Always test with sample data in dev environment
   - Verify column names and types match

2. **Backup Strategy**
   - CSV files are auto-backed up to Volume
   - Keep original CSV files locally too
   - Document upload dates and sources

3. **Data Quality**
   - Validate CSV before upload
   - Check for proper encoding (UTF-8)
   - Remove extra commas or special characters

4. **Incremental Uploads**
   - Use "Append" mode for regular updates
   - Track which records are new vs existing
   - Consider adding upload_batch_id column

5. **Metadata Tracking**
   - Always enable metadata columns
   - Helps with audit trail
   - Useful for troubleshooting

## File Locations

**Uploaded CSV backups:**
```
/Volumes/dg_dev/sandbox/csv_uploads/filename_YYYY-MM-DD_HH-MM-SS.csv
```

**Target tables:**
```
dg_dev.sandbox.out_merchant_business_size_for_bank (Dev)
dg_prod.sandbox.out_merchant_business_size_for_bank_test (Prod Test)
```

## Next Steps

1. **Try the sample file:**
   ```bash
   # Use sample_merchant_data.csv included in project
   ```

2. **Create your own CSV:**
   - Export from your source system
   - Ensure column names match
   - Test with small batch first

3. **Set up regular uploads:**
   - Schedule CSV exports
   - Upload weekly/monthly
   - Monitor upload success

4. **Monitor and maintain:**
   - Check Volume storage usage
   - Archive old CSV backups
   - Review upload logs

## Support

For issues or questions:
- Check the "Help" tab in the CSV Upload page
- Review error messages carefully
- Contact data governance team
- Check Databricks workspace logs

## Summary

The CSV upload feature transforms your workflow from:
- ❌ Manual entry of each record
- ❌ Time-consuming data entry
- ❌ Higher error rates

To:
- ✅ Bulk upload hundreds of records
- ✅ Fast and efficient
- ✅ Automatic validation and backup

Happy uploading! 🚀
