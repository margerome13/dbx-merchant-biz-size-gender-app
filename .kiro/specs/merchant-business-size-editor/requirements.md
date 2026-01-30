# Requirements Document

## Introduction

This document specifies the requirements for adapting the existing Databricks table editor application to manage the `dg_dev.sandbox.out_merchant_business_size_for_bank` table. The application will provide a form-based interface for CRUD operations on merchant business size data.

## Glossary

- **Editor_Application**: The Streamlit-based web application that provides form-based table editing
- **Target_Table**: The Databricks table `dg_dev.sandbox.out_merchant_business_size_for_bank`
- **Databricks_Warehouse**: The SQL warehouse endpoint used to connect to Databricks
- **Form_Field**: An input element in the user interface for editing table columns
- **CRUD_Operations**: Create, Read, Update, and Delete operations on table records
- **Session_State**: Streamlit's mechanism for maintaining state across user interactions
- **GitHub_Repository**: The remote repository at https://github.com/margerome13/dbx-table-read-form-app-maker-checker

## Requirements

### Requirement 1: Table Configuration

**User Story:** As a data administrator, I want the application to connect to the merchant business size table, so that I can manage merchant data instead of MDAR inventory data.

#### Acceptance Criteria

1. WHEN the application starts, THE Editor_Application SHALL connect to `dg_dev.sandbox.out_merchant_business_size_for_bank`
2. WHEN the connection is established, THE Editor_Application SHALL retrieve the table schema dynamically
3. WHEN displaying connection details, THE Editor_Application SHALL show the correct table name to the user

### Requirement 2: View Configuration

**User Story:** As a user, I want to see the merchant business size editor in the navigation, so that I can access the correct editing interface.

#### Acceptance Criteria

1. WHEN the application loads, THE Editor_Application SHALL display "Merchant Business Size Editor" as the view label
2. WHEN viewing the page title, THE Editor_Application SHALL show "Merchant Business Size for Bank" as the header
3. WHEN viewing help text, THE Editor_Application SHALL describe the merchant business size table being edited

### Requirement 3: Dynamic Form Generation

**User Story:** As a data administrator, I want the form fields to match the actual table schema, so that I can edit all available columns.

#### Acceptance Criteria

1. WHEN the table schema is retrieved, THE Editor_Application SHALL generate form fields for each column
2. WHEN a column has a specific data type, THE Editor_Application SHALL render the appropriate input widget
3. WHEN the table schema changes, THE Editor_Application SHALL reflect those changes without code modifications

### Requirement 4: CRUD Operations

**User Story:** As a data administrator, I want to create, read, update, and delete merchant records, so that I can maintain accurate business size data.

#### Acceptance Criteria

1. WHEN adding a new record, THE Editor_Application SHALL insert the record into the Target_Table
2. WHEN editing an existing record, THE Editor_Application SHALL update the record in the Target_Table
3. WHEN deleting a record, THE Editor_Application SHALL remove the record from the Target_Table
4. WHEN reading records, THE Editor_Application SHALL display current data from the Target_Table

### Requirement 5: Data Validation

**User Story:** As a data administrator, I want the application to validate data before saving, so that I maintain data quality.

#### Acceptance Criteria

1. WHEN submitting a form, THE Editor_Application SHALL validate required fields are not empty
2. WHEN validation fails, THE Editor_Application SHALL display clear error messages
3. WHEN validation succeeds, THE Editor_Application SHALL proceed with the database operation

### Requirement 6: Search and Filter

**User Story:** As a data administrator, I want to search and filter records, so that I can quickly find specific merchant data.

#### Acceptance Criteria

1. WHEN entering a search term, THE Editor_Application SHALL filter displayed records across all columns
2. WHEN the search term matches records, THE Editor_Application SHALL display the count of matching records
3. WHEN clearing the search, THE Editor_Application SHALL display all records again

### Requirement 7: Version Control Integration

**User Story:** As a developer, I want changes pushed to the GitHub repository, so that the codebase is version controlled and deployable.

#### Acceptance Criteria

1. WHEN code changes are complete, THE Editor_Application SHALL be committed to the local git repository
2. WHEN commits are ready, THE Editor_Application SHALL be pushed to the GitHub_Repository
3. WHEN pushing to GitHub, THE Editor_Application SHALL use the correct remote URL

### Requirement 8: Configuration Removal

**User Story:** As a developer, I want MDAR-specific configurations removed, so that the application is clean and focused on merchant data.

#### Acceptance Criteria

1. WHEN reviewing the code, THE Editor_Application SHALL NOT contain MDAR-specific dropdown values
2. WHEN reviewing the code, THE Editor_Application SHALL NOT contain MDAR-specific validation logic
3. WHEN reviewing the code, THE Editor_Application SHALL NOT contain MDAR-specific field configurations
