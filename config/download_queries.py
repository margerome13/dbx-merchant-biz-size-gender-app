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
with mambu_groups as (
    select
        *,
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
            ",",1) as cpm_id
    from de_maya_prod.dlake_maya_customers__epm.z1_mambu__groups mambu
),
sfdc as (
    select * from de_prod.dlake_customers__epm.z1_salesforce__account
),
amanda as (
    select * from de_prod.dlake_customers__epm.z1_amanda_user__ams_merchant
),

mambu_deposit_prods as (
    select
        encoded_key,
        name,
        product_type,
        creation_date,
        last_modified_date
    from de_maya_prod.dlake_maya_products__savings.z1_mambu__deposit_products
),
mambu_deposit_accts as (
    select
        *,
        regexp_extract(
            regexp_replace(regexp_replace(regexp_replace(
                custom_fields,
                '\\\\[|\\\\]|\\\\{|\\\\}|',''),
                char(39),''),
                ', value',''),
            'depositAccountNumber: ([0-9]+)',1) as account_number,
        date_format(from_utc_timestamp(creation_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') as creation_date_pht,
        date_format(from_utc_timestamp(activation_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') as activation_date_pht,
        date_format(from_utc_timestamp(closed_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') as closed_date_pht,
        date_format(from_utc_timestamp(locked_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') as locked_date_pht,
        date_format(from_utc_timestamp(maturity_date,'Asia/Manila'),'yyyy-MM-dd HH:mm:ss') as maturity_date_pht
    from de_maya_prod.dlake_maya_accounts__las.z1_mambu__deposit_accounts
),
lms_credit as (
    select distinct
        owner_id as customer_id,
        product_key,
        FROM_UTC_TIMESTAMP(created_date,'Asia/Manila') as created_date_pht
    from de_maya_prod.dlake_maya_products__lending.z1h_loan_account__loan_accounts
    where true
    union all
    select distinct
        customer_id,
        product_key,
        FROM_UTC_TIMESTAMP(created_date,'Asia/Manila') as created_date_pht
    from de_maya_prod.dlake_maya_products__lending.z1_loan_account__credit_arrangements
),
cpm as (
    select
        *,
        split_part(
            regexp_extract(
                regexp_replace(
                    OrganizationDetails,
                    '\\\\[|\\\\]|\\\\{|\\\\}|\\\\"',''),
                'natureOfOrganization:\\\\s*(.*)\\\\s*,',1),
            ",", 1) nature_of_organization
    from de_prod.dlake_customers__epm.z1_cpm__organization
),

final as (
    select distinct
        lms.customer_id as cpm_id,
        sf.name as business_name,
        sf.DBA_Trade_Name__c as trade_name,
        sf.id as sf_id,
        mer.id as amanda_id,
        lms.product_key as loan_product,
        sf.Business_Size__c as sf_business_size,
        mer.business_size as amanda_business_size,
        mdm.asset_size,
        sf.Merchant_Category_Code__c as sf_mcc,
        cpm.nature_of_organization as cpm_nature_of_organization,
        coalesce(cbs.gender,cpm_gender.value) as gender,
        '' as business_reviewed_size,
        '' as business_reviewed_gender,
        '' as reviewed_by,
        '' as business_reviewed_date,
        date_format(from_utc_timestamp(now(),'Asia/Manila'),'yyyy-MM-dd') dq_execution_date
    from lms_credit lms
    left join sfdc sf on sf.CPM_Account_ID__c = lms.customer_id
    left join amanda mer on mer.cpm_id = lms.customer_id
    left join mambu_deposit_accts dep_accts
        on dep_accts.account_holder_key = lms.customer_id
    left join mambu_deposit_prods prods
        on prods.encoded_key = dep_accts.product_type_key
    left join cpm cpm
        on cpm.id = lms.customer_id
    left join de_prod.dlake_customers__epm.z2_cpm__person_gender cpm_gender
        on cpm_gender.person_id = lms.customer_id
    left join dg_prod.z3_edw.dim_customer_merchant mdm
        on mdm.customer_id = sf.CPM_Account_ID__c
    left join cbs_prod.z3_regrep_counterparty.sme_line_borrower_info cbs
        on cbs.IDE_COUNTERPARTY_REF = replace(lms.customer_id,'-','')
    where true
)
select distinct *
from final
where true
    and loan_product = 'MAYA_FLEXI_ENTERPRISE_LOAN'
    and (
        coalesce(asset_size,sf_business_size,amanda_business_size) is null
        or coalesce(asset_size,sf_business_size,amanda_business_size) = 'LARGE'
        or gender is null
    )
ORDER BY BUSINESS_NAME ASC
"""
    }
}
