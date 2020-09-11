WITH all_pct_clean AS (
    SELECT id
        ,ref_year
        ,code
        ,geo_name
        ,geo_parent_name
        ,pop15p
        ,CASE WHEN pop15p = 0 THEN 0 ELSE lbf / pop15p END     AS lbf
        ,CASE WHEN lbf = 0 THEN 0 ELSE employed / lbf END      AS employed
        ,CASE WHEN lbf = 0 THEN 0 ELSE employed_full / lbf END AS employed_full
        ,CASE WHEN lbf = 0 THEN 0 ELSE employed_part / lbf END AS employed_part
        ,CASE WHEN lbf = 0 THEN 0 ELSE unemployed / lbf END    AS unemployed
        ,CASE WHEN lbf = 0 THEN 0 ELSE not_in_lbf / pop15p END AS not_in_lbf
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_goods / employed END                 AS naics_goods
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_agriculture / employed END           AS naics_agriculture
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_natural_resources / employed END     AS naics_natural_resources
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_ultilities / employed END            AS naics_ultilities
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_construction / employed END          AS naics_construction
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_manufacturing / employed END         AS naics_manufacturing
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_services / employed END              AS naics_services
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_retail / employed END                AS naics_retail
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_transport_warehousing / employed END AS naics_transport_warehousing
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_financial / employed END             AS naics_financial
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_professional / employed END          AS naics_professional
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_support_services / employed END      AS naics_support_services
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_education_services / employed END    AS naics_education_services
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_health / employed END                AS naics_health
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_infor_culture / employed END         AS naics_infor_culture
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_accomdation_food / employed END      AS naics_accomdation_food
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_other / employed END                 AS naics_other
        ,CASE WHEN employed = 0 THEN 0 ELSE naics_public_admin / employed END          AS naics_public_admin
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_management / employed END               AS nocs_management
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_management_senior / employed END        AS nocs_management_senior
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_management_specialized / employed END   AS nocs_management_specialized
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_management_retail / employed END        AS nocs_management_retail
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_management_trades / employed END        AS nocs_management_trades
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_business / employed END                 AS nocs_business
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_business_pro / employed END             AS nocs_business_pro
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_business_admin / employed END           AS nocs_business_admin
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_business_finance / employed END         AS nocs_business_finance
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_business_office / employed END          AS nocs_business_office
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_business_distribution / employed END    AS nocs_business_distribution
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sciences / employed END                 AS nocs_sciences
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sciences_pro / employed END             AS nocs_sciences_pro
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sciences_tech / employed END            AS nocs_sciences_tech
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_health / employed END                   AS nocs_health
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_health_nursing / employed END           AS nocs_health_nursing
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_health_pro / employed END               AS nocs_health_pro
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_health_tech / employed END              AS nocs_health_tech
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_health_assist / employed END            AS nocs_health_assist
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_public / employed END                   AS nocs_public
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_public_education / employed END         AS nocs_public_education
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_public_pro / employed END               AS nocs_public_pro
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_public_para / employed END              AS nocs_public_para
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_public_protection / employed END        AS nocs_public_protection
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_public_support / employed END           AS nocs_public_support
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_culture / employed END                  AS nocs_culture
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_culture_pro / employed END              AS nocs_culture_pro
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_culture_tech / employed END             AS nocs_culture_tech
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales / employed END                    AS nocs_sales
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales_retail / employed END             AS nocs_sales_retail
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales_service / employed END            AS nocs_sales_service
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales_wholesale / employed END          AS nocs_sales_wholesale
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales_other / employed END              AS nocs_sales_other
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales_support / employed END            AS nocs_sales_support
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_sales_nec / employed END                AS nocs_sales_nec
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_trades / employed END                   AS nocs_trades
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_trades_industrial / employed END        AS nocs_trades_industrial
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_trades_maintenance / employed END       AS nocs_trades_maintenance
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_trades_other / employed END             AS nocs_trades_other
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_trades_heavy / employed END             AS nocs_trades_heavy
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_trades_helpers / employed END           AS nocs_trades_helpers
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_agriculture / employed END              AS nocs_agriculture
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_agriculture_supervisor / employed END   AS nocs_agriculture_supervisor
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_agriculture_workers / employed END      AS nocs_agriculture_workers
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_agriculture_harvesting / employed END   AS nocs_agriculture_harvesting
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_manufacturing / employed END            AS nocs_manufacturing
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_manufacturing_supervisor / employed END AS nocs_manufacturing_supervisor
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_manufacturing_operators / employed END  AS nocs_manufacturing_operators
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_manufacturing_assemblers / employed END AS nocs_manufacturing_assemblers
        ,CASE WHEN employed = 0 THEN 0 ELSE nocs_manufacturing_labourers / employed END  AS nocs_manufacturing_labourers
    FROM {{ref('all_wide_clean')}}
)
SELECT * FROM all_pct_clean