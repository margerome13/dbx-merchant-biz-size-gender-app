# Implementation Plan: Merchant Business Size Editor

## Overview

This plan adapts the existing MDAR table editor to work with the merchant business size table. The implementation involves updating configuration files, removing MDAR-specific code, and pushing changes to GitHub.

## Tasks

- [x] 1. Update view registry configuration
  - Modify `view_groups.py` to reference merchant business size editor
  - Update label to "Merchant Business Size Editor"
  - Update help text to describe merchant table
  - Update icon to `:material/store:`
  - _Requirements: 2.1, 2.2, 2.3_

- [x] 2. Create new merchant business size editor view
  - [x] 2.1 Copy existing editor and update table configuration
    - Copy `views/dq_mdar_masterfile_editor.py` to `views/merchant_business_size_editor.py`
    - Update `TABLE_NAME` constant to `dg_dev.sandbox.out_merchant_business_size_for_bank`
    - Update page header to "Merchant Business Size for Bank"
    - Update description text to reference merchant table
    - _Requirements: 1.1, 1.3, 2.2_
  
  - [x] 2.2 Remove MDAR-specific configurations
    - Remove `DROPDOWN_VALUES` dictionary (MDAR-specific dropdowns)
    - Remove `MULTILINE_FIELDS` list (MDAR-specific)
    - Remove `validate_ticket_format()` function
    - Remove `check_ticket_exists()` function
    - Remove Manila timezone functions (`get_manila_timestamp()`)
    - Remove timestamp field special handling in `render_form_field()`
    - Remove ticket field special handling in `render_form_field()`
    - _Requirements: 8.1, 8.2, 8.3_
  
  - [x] 2.3 Simplify validation logic
    - Update `validate_new_record()` to generic validation (check for empty fields only)
    - Remove timeline conditional logic
    - Remove ticket existence check from add record flow
    - _Requirements: 5.1, 5.2, 8.2_
  
  - [x] 2.4 Update form field rendering
    - Simplify `render_form_field()` to remove dropdown lookups
    - Remove special handling for created_pht/updated_pht fields
    - Remove special handling for ticket field
    - Keep generic type-based rendering (int, float, boolean, date, timestamp, string)
    - _Requirements: 3.1, 3.2_
  
  - [x] 2.5 Update insert and update functions
    - Remove Manila timestamp injection in `insert_record()`
    - Remove Manila timestamp update in `update_record()`
    - Remove ticket validation from both functions
    - Keep SQL escaping and NULL handling
    - _Requirements: 4.1, 4.2_

- [x] 3. Remove old MDAR editor view
  - Delete `views/dq_mdar_masterfile_editor.py`
  - _Requirements: 8.1_

- [x] 4. Checkpoint - Test the application locally
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Configure git and push changes
  - [x] 5.1 Check git remote configuration
    - Verify remote URL is set to `https://github.com/margerome13/dbx-table-read-form-app-maker-checker`
    - Add remote if not configured
    - _Requirements: 7.3_
  
  - [x] 5.2 Stage and commit changes
    - Stage all modified files (`view_groups.py`, new editor view)
    - Stage deleted file (old MDAR editor)
    - Create commit with message: "Adapt editor for merchant business size table"
    - _Requirements: 7.1_
  
  - [x] 5.3 Push to GitHub
    - Push commits to remote repository
    - Verify push succeeds
    - _Requirements: 7.2_

- [x] 6. Final checkpoint - Verify deployment
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- The application uses dynamic schema discovery, so no hardcoded field definitions are needed
- All MDAR-specific logic (ticket validation, dropdown values, timezone handling) should be removed
- The core CRUD functionality and form rendering logic remains unchanged
- Git operations should be performed after code changes are complete and tested
