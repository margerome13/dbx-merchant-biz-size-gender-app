"""
SQL Query Configuration for CSV Download Feature.
Update queries here when criteria change — no need to modify the app view code.
"""

DOWNLOAD_QUERIES = {
    "merchant_business_size_for_bank": {
        "label": "Merchant Business Size for Bank",
        "description": "Generates merchant business size data for bank reporting. Filters for MAYA_FLEXI_ENTERPRISE_LOAN with missing/LARGE asset size or missing gender.",
        "target_table": "dg_dev.sandbox.out_merchant_business_size_for_bank",
        "sql": """
CREATE OR REPLACE TABLE dg_dev.sandbox.out_merchant_business_size_for_bank AS
WITH mambu_groups AS (
    SELECT *,
        split_part(
            regexp_extract(
                regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                    custom_fields,
                    '\\\\[|\\\\]|\\\\{|\\\\}|, value',''),
                    '":"',': '),
                    '","',', '),
                    '^"|"$',''),
                    char(39),''),
                    ', value',''),
                'organizationCpmId: (.*)',1),
            ',',1) AS cpm_id
    FROM de_maya_prod.dlake_maya_customers__epm.z1_mambu__groups mambu
),
sfdc AS (
    SELECT * FROM de_prod.dlake_customers__epm.z1_salesforce__account
),
amanda AS (
    SELECT * FROM de_prod.dlake_customers__epm.z1_amanda_user__ams_merchant
),
mambu_deposit_prods AS (
    SELECT encoded_key, name, product_type, creation_date, last_modified_date
    FROM de_maya_prod.dlake_maya_products__savings.z1_mambu__deposit_products
),
mambu_deposit_accts AS (
    SELECT *,
        regexp_extract(
            regexp_replace(regexp_replace(regexp_replace(
                custom_fields,
                '\\\\[|\\\\]|\\\\{|\\\\}|',''),
                char(39),''),
                ', value',''),
            'depositAccountNumber: ([0-9]+)',1) AS account_number,
        date_format(from_utc_timestamp(creation_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') AS creation_date_pht,
        date_format(from_utc_timestamp(activation_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') AS activation_date_pht,
        date_format(from_utc_timestamp(closed_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') AS closed_date_pht,
        date_format(from_utc_timestamp(locked_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') AS locked_date_pht,
        date_format(from_utc_timestamp(maturity_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') AS maturity_date_pht
    FROM de_maya_prod.dlake_maya_accounts__las.z1_mambu__deposit_accounts
),
lms_credit AS (
    SELECT DISTINCT
        owner_id AS customer_id,
        product_key,
        FROM_UTC_TIMESTAMP(created_date,'Asia/Manila') AS created_date_pht
    FROM de_maya_prod.dlake_maya_products__lending.z1h_loan_account__loan_accounts
    WHERE true
    UNION ALL
    SELECT DISTINCT
        customer_id,
        product_key,
        FROM_UTC_TIMESTAMP(created_date,'Asia/Manila') AS created_date_pht
    FROM de_maya_prod.dlake_maya_products__lending.z1_loan_account__credit_arrangements
),
cpm AS (
    SELECT *,
        upper(split_part(
            regexp_extract(
                regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                    OrganizationDetails,
                    '\\\\[|\\\\]|\\\\{|\\\\}',''),
                    '":"',': '),
                    '","',', '),
                    '^"|"$',''),
                'natureOfOrganization: (.*),',1),
            ',',1)) AS nature_of_organization
    FROM de_prod.dlake_customers__epm.z1_cpm__organization
),
final AS (
    SELECT DISTINCT
        lms.customer_id AS cpm_id,
        sf.name AS business_name,
        sf.DBA_Trade_Name__c AS trade_name,
        sf.id AS sf_id,
        mer.id AS amanda_id,
        lms.product_key AS loan_product,
        sf.Business_Size__c AS sf_business_size,
        mer.business_size AS amanda_business_size,
        mdm.asset_size,
        sf.Merchant_Category_Code__c AS sf_mcc,
        cpm.nature_of_organization AS cpm_nature_of_organization,
        coalesce(cbs.gender, cpm_gender.value) AS gender,
        '' AS business_reviewed_size,
        '' AS business_reviewed_gender,
        '' AS reviewed_by,
        '' AS business_reviewed_date,
        date_format(from_utc_timestamp(now(),'Asia/Manila'),'yyyy-MM-dd') AS dq_execution_date
    FROM lms_credit lms
    LEFT JOIN sfdc sf ON sf.CPM_Account_ID__c = lms.customer_id
    LEFT JOIN amanda mer ON mer.cpm_id = lms.customer_id
    LEFT JOIN mambu_deposit_accts dep_accts ON dep_accts.account_holder_key = lms.customer_id
    LEFT JOIN mambu_deposit_prods prods ON prods.encoded_key = dep_accts.product_type_key
    LEFT JOIN cpm cpm ON cpm.id = lms.customer_id
    LEFT JOIN de_prod.dlake_customers__epm.z2_cpm__person_gender cpm_gender ON cpm_gender.person_id = lms.customer_id
    LEFT JOIN dg_prod.z3_edw.dim_customer_merchant mdm ON mdm.customer_id = sf.CPM_Account_ID__c
    LEFT JOIN cbs_prod.z3_regrep_counterparty.sme_line_borrower_info cbs ON cbs.IDE_COUNTERPARTY_REF = replace(lms.customer_id,'-','')
    WHERE true
)
SELECT DISTINCT *
FROM final
WHERE true
    AND loan_product = 'MAYA_FLEXI_ENTERPRISE_LOAN'
    AND (
        coalesce(asset_size, sf_business_size, amanda_business_size) IS NULL
        OR coalesce(asset_size, sf_business_size, amanda_business_size) = 'LARGE'
        OR gender IS NULL
    )
ORDER BY BUSINESS_NAME ASC
"""
    }
}
