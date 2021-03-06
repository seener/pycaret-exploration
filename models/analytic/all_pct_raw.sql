WITH all_pct_raw AS (
    SELECT id
        ,ref_year
        ,code
        ,geo_name
        ,geo_parent_name
        ,geo_level_name
        ,pop15p
        ,CASE WHEN pop15p = 0 THEN 0 ELSE lbf / pop15p END                                         AS lbf
        ,CASE WHEN lbf = 0 THEN 0 ELSE employed / lbf END                                          AS employed
        ,CASE WHEN lbf = 0 THEN 0 ELSE employed_full / lbf END                                     AS employed_full
        ,CASE WHEN lbf = 0 THEN 0 ELSE employed_part / lbf END                                     AS employed_part
        ,CASE WHEN lbf = 0 THEN 0 ELSE unemployed / lbf END                                        AS unemployed
        ,CASE WHEN lbf = 0 THEN 0 ELSE not_in_lbf / pop15p END                                     AS not_in_lbf
        ,CASE WHEN lbf = 0 THEN 0 ELSE naics_employed / lbf END                                    AS naics_employed 
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_goods / naics_employed END                 AS naics_goods
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_agriculture / naics_employed END           AS naics_agriculture
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_natural_resources / naics_employed END     AS naics_natural_resources
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_ultilities / naics_employed END            AS naics_ultilities
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_construction / naics_employed END          AS naics_construction
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_manufacturing / naics_employed END         AS naics_manufacturing
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_services / naics_employed END              AS naics_services
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_retail / naics_employed END                AS naics_retail
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_transport_warehousing / naics_employed END AS naics_transport_warehousing
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_financial / naics_employed END             AS naics_financial
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_professional / naics_employed END          AS naics_professional
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_support_services / naics_employed END      AS naics_support_services
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_education_services / naics_employed END    AS naics_education_services
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_health / naics_employed END                AS naics_health
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_infor_culture / naics_employed END         AS naics_infor_culture
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_accomdation_food / naics_employed END      AS naics_accomdation_food
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_other / naics_employed END                 AS naics_other
        ,CASE WHEN naics_employed = 0 THEN 0 ELSE naics_public_admin / naics_employed END          AS naics_public_admin
        ,CASE WHEN lbf = 0 THEN 0 ELSE nocs_employed / lbf END                                     AS nocs_employed
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_management / nocs_employed END               AS nocs_management
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_management_senior / nocs_employed END        AS nocs_management_senior
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_management_specialized / nocs_employed END   AS nocs_management_specialized
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_management_retail / nocs_employed END        AS nocs_management_retail
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_management_trades / nocs_employed END        AS nocs_management_trades
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_business / nocs_employed END                 AS nocs_business
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_business_pro / nocs_employed END             AS nocs_business_pro
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_business_admin / nocs_employed END           AS nocs_business_admin
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_business_finance / nocs_employed END         AS nocs_business_finance
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_business_office / nocs_employed END          AS nocs_business_office
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_business_distribution / nocs_employed END    AS nocs_business_distribution
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sciences / nocs_employed END                 AS nocs_sciences
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sciences_pro / nocs_employed END             AS nocs_sciences_pro
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sciences_tech / nocs_employed END            AS nocs_sciences_tech
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_health / nocs_employed END                   AS nocs_health
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_health_nursing / nocs_employed END           AS nocs_health_nursing
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_health_pro / nocs_employed END               AS nocs_health_pro
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_health_tech / nocs_employed END              AS nocs_health_tech
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_health_assist / nocs_employed END            AS nocs_health_assist
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_public / nocs_employed END                   AS nocs_public
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_public_education / nocs_employed END         AS nocs_public_education
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_public_pro / nocs_employed END               AS nocs_public_pro
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_public_para / nocs_employed END              AS nocs_public_para
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_public_protection / nocs_employed END        AS nocs_public_protection
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_public_support / nocs_employed END           AS nocs_public_support
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_culture / nocs_employed END                  AS nocs_culture
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_culture_pro / nocs_employed END              AS nocs_culture_pro
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_culture_tech / nocs_employed END             AS nocs_culture_tech
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales / nocs_employed END                    AS nocs_sales
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales_retail / nocs_employed END             AS nocs_sales_retail
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales_service / nocs_employed END            AS nocs_sales_service
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales_wholesale / nocs_employed END          AS nocs_sales_wholesale
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales_other / nocs_employed END              AS nocs_sales_other
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales_support / nocs_employed END            AS nocs_sales_support
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_sales_nec / nocs_employed END                AS nocs_sales_nec
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_trades / nocs_employed END                   AS nocs_trades
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_trades_industrial / nocs_employed END        AS nocs_trades_industrial
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_trades_maintenance / nocs_employed END       AS nocs_trades_maintenance
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_trades_other / nocs_employed END             AS nocs_trades_other
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_trades_heavy / nocs_employed END             AS nocs_trades_heavy
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_trades_helpers / nocs_employed END           AS nocs_trades_helpers
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_agriculture / nocs_employed END              AS nocs_agriculture
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_agriculture_supervisor / nocs_employed END   AS nocs_agriculture_supervisor
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_agriculture_workers / nocs_employed END      AS nocs_agriculture_workers
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_agriculture_harvesting / nocs_employed END   AS nocs_agriculture_harvesting
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_manufacturing / nocs_employed END            AS nocs_manufacturing
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_manufacturing_supervisor / nocs_employed END AS nocs_manufacturing_supervisor
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_manufacturing_operators / nocs_employed END  AS nocs_manufacturing_operators
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_manufacturing_assemblers / nocs_employed END AS nocs_manufacturing_assemblers
        ,CASE WHEN nocs_employed = 0 THEN 0 ELSE nocs_manufacturing_labourers / nocs_employed END  AS nocs_manufacturing_labourers
    FROM {{ref('all_wide_raw')}}
)
SELECT * FROM all_pct_raw