WITH all_wide_er AS (
    SELECT  *
    FROM {{ ref('all_wide_raw') }}
    WHERE geo_level_name = 'Economic Region' 
),
all_wide_t_minus AS (
    SELECT  p0.id
        ,CASE WHEN SUM( m1.lbf ) < 0.0001 THEN 0.0001 ELSE SUM( m1.lbf ) END                                                     AS tm_lbf
        ,CASE WHEN SUM( m1.employed ) < 0.0001 THEN 0.0001 ELSE SUM( m1.employed ) END                                           AS tm_employed
        ,CASE WHEN SUM( m1.employed_full ) < 0.0001 THEN 0.0001 ELSE SUM( m1.employed_full ) END                                 AS tm_employed_full
        ,CASE WHEN SUM( m1.employed_part ) < 0.0001 THEN 0.0001 ELSE SUM( m1.employed_part ) END                                 AS tm_employed_part
        ,CASE WHEN SUM( m1.unemployed ) < 0.0001 THEN 0.0001 ELSE SUM( m1.unemployed ) END                                       AS tm_unemployed
        ,CASE WHEN SUM( m1.not_in_lbf ) < 0.0001 THEN 0.0001 ELSE SUM( m1.not_in_lbf ) END                                       AS tm_not_in_lbf
        ,CASE WHEN SUM( m1.naics_employed ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_employed ) END                               AS tm_naics_employed
        ,CASE WHEN SUM( m1.naics_goods ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_goods ) END                                     AS tm_naics_goods
        ,CASE WHEN SUM( m1.naics_agriculture ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_agriculture ) END                         AS tm_naics_agriculture
        ,CASE WHEN SUM( m1.naics_natural_resources ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_natural_resources ) END             AS tm_naics_natural_resources
        ,CASE WHEN SUM( m1.naics_ultilities ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_ultilities ) END                           AS tm_naics_ultilities
        ,CASE WHEN SUM( m1.naics_construction ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_construction ) END                       AS tm_naics_construction
        ,CASE WHEN SUM( m1.naics_manufacturing ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_manufacturing ) END                     AS tm_naics_manufacturing
        ,CASE WHEN SUM( m1.naics_services ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_services ) END                               AS tm_naics_services
        ,CASE WHEN SUM( m1.naics_retail ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_retail ) END                                   AS tm_naics_retail
        ,CASE WHEN SUM( m1.naics_transport_warehousing ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_transport_warehousing ) END     AS tm_naics_transport_warehousing
        ,CASE WHEN SUM( m1.naics_financial ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_financial ) END                             AS tm_naics_financial
        ,CASE WHEN SUM( m1.naics_professional ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_professional ) END                       AS tm_naics_professional
        ,CASE WHEN SUM( m1.naics_support_services ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_support_services ) END               AS tm_naics_support_services
        ,CASE WHEN SUM( m1.naics_education_services ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_education_services ) END           AS tm_naics_education_services
        ,CASE WHEN SUM( m1.naics_health ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_health ) END                                   AS tm_naics_health
        ,CASE WHEN SUM( m1.naics_infor_culture ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_infor_culture ) END                     AS tm_naics_infor_culture
        ,CASE WHEN SUM( m1.naics_accomdation_food ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_accomdation_food ) END               AS tm_naics_accomdation_food
        ,CASE WHEN SUM( m1.naics_other ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_other ) END                                     AS tm_naics_other
        ,CASE WHEN SUM( m1.naics_public_admin ) < 0.0001 THEN 0.0001 ELSE SUM( m1.naics_public_admin ) END                       AS tm_naics_public_admin
        ,CASE WHEN SUM( m1.nocs_employed ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_employed ) END                                 AS tm_nocs_employed
        ,CASE WHEN SUM( m1.nocs_management ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_management ) END                             AS tm_nocs_management
        ,CASE WHEN SUM( m1.nocs_management_senior ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_management_senior ) END               AS tm_nocs_management_senior
        ,CASE WHEN SUM( m1.nocs_management_specialized ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_management_specialized ) END     AS tm_nocs_management_specialized
        ,CASE WHEN SUM( m1.nocs_management_retail ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_management_retail ) END               AS tm_nocs_management_retail
        ,CASE WHEN SUM( m1.nocs_management_trades ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_management_trades ) END               AS tm_nocs_management_trades
        ,CASE WHEN SUM( m1.nocs_business ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_business ) END                                 AS tm_nocs_business
        ,CASE WHEN SUM( m1.nocs_business_pro ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_business_pro ) END                         AS tm_nocs_business_pro
        ,CASE WHEN SUM( m1.nocs_business_admin ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_business_admin ) END                     AS tm_nocs_business_admin
        ,CASE WHEN SUM( m1.nocs_business_finance ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_business_finance ) END                 AS tm_nocs_business_finance
        ,CASE WHEN SUM( m1.nocs_business_office ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_business_office ) END                   AS tm_nocs_business_office
        ,CASE WHEN SUM( m1.nocs_business_distribution ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_business_distribution ) END       AS tm_nocs_business_distribution
        ,CASE WHEN SUM( m1.nocs_sciences ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sciences ) END                                 AS tm_nocs_sciences
        ,CASE WHEN SUM( m1.nocs_sciences_pro ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sciences_pro ) END                         AS tm_nocs_sciences_pro
        ,CASE WHEN SUM( m1.nocs_sciences_tech ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sciences_tech ) END                       AS tm_nocs_sciences_tech
        ,CASE WHEN SUM( m1.nocs_health ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_health ) END                                     AS tm_nocs_health
        ,CASE WHEN SUM( m1.nocs_health_nursing ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_health_nursing ) END                     AS tm_nocs_health_nursing
        ,CASE WHEN SUM( m1.nocs_health_pro ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_health_pro ) END                             AS tm_nocs_health_pro
        ,CASE WHEN SUM( m1.nocs_health_tech ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_health_tech ) END                           AS tm_nocs_health_tech
        ,CASE WHEN SUM( m1.nocs_health_assist ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_health_assist ) END                       AS tm_nocs_health_assist
        ,CASE WHEN SUM( m1.nocs_public ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_public ) END                                     AS tm_nocs_public
        ,CASE WHEN SUM( m1.nocs_public_education ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_public_education ) END                 AS tm_nocs_public_education
        ,CASE WHEN SUM( m1.nocs_public_pro ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_public_pro ) END                             AS tm_nocs_public_pro
        ,CASE WHEN SUM( m1.nocs_public_para ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_public_para ) END                           AS tm_nocs_public_para
        ,CASE WHEN SUM( m1.nocs_public_protection ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_public_protection ) END               AS tm_nocs_public_protection
        ,CASE WHEN SUM( m1.nocs_public_support ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_public_support ) END                     AS tm_nocs_public_support
        ,CASE WHEN SUM( m1.nocs_culture ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_culture ) END                                   AS tm_nocs_culture
        ,CASE WHEN SUM( m1.nocs_culture_pro ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_culture_pro ) END                           AS tm_nocs_culture_pro
        ,CASE WHEN SUM( m1.nocs_culture_tech ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_culture_tech ) END                         AS tm_nocs_culture_tech
        ,CASE WHEN SUM( m1.nocs_sales ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales ) END                                       AS tm_nocs_sales
        ,CASE WHEN SUM( m1.nocs_sales_retail ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales_retail ) END                         AS tm_nocs_sales_retail
        ,CASE WHEN SUM( m1.nocs_sales_service ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales_service ) END                       AS tm_nocs_sales_service
        ,CASE WHEN SUM( m1.nocs_sales_wholesale ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales_wholesale ) END                   AS tm_nocs_sales_wholesale
        ,CASE WHEN SUM( m1.nocs_sales_other ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales_other ) END                           AS tm_nocs_sales_other
        ,CASE WHEN SUM( m1.nocs_sales_support ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales_support ) END                       AS tm_nocs_sales_support
        ,CASE WHEN SUM( m1.nocs_sales_nec ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_sales_nec ) END                               AS tm_nocs_sales_nec
        ,CASE WHEN SUM( m1.nocs_trades ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_trades ) END                                     AS tm_nocs_trades
        ,CASE WHEN SUM( m1.nocs_trades_industrial ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_trades_industrial ) END               AS tm_nocs_trades_industrial
        ,CASE WHEN SUM( m1.nocs_trades_maintenance ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_trades_maintenance ) END             AS tm_nocs_trades_maintenance
        ,CASE WHEN SUM( m1.nocs_trades_other ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_trades_other ) END                         AS tm_nocs_trades_other
        ,CASE WHEN SUM( m1.nocs_trades_heavy ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_trades_heavy ) END                         AS tm_nocs_trades_heavy
        ,CASE WHEN SUM( m1.nocs_trades_helpers ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_trades_helpers ) END                     AS tm_nocs_trades_helpers
        ,CASE WHEN SUM( m1.nocs_agriculture ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_agriculture ) END                           AS tm_nocs_agriculture
        ,CASE WHEN SUM( m1.nocs_agriculture_supervisor ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_agriculture_supervisor ) END     AS tm_nocs_agriculture_supervisor
        ,CASE WHEN SUM( m1.nocs_agriculture_workers ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_agriculture_workers ) END           AS tm_nocs_agriculture_workers
        ,CASE WHEN SUM( m1.nocs_agriculture_harvesting ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_agriculture_harvesting ) END     AS tm_nocs_agriculture_harvesting
        ,CASE WHEN SUM( m1.nocs_manufacturing ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_manufacturing ) END                       AS tm_nocs_manufacturing
        ,CASE WHEN SUM( m1.nocs_manufacturing_supervisor ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_manufacturing_supervisor ) END AS tm_nocs_manufacturing_supervisor
        ,CASE WHEN SUM( m1.nocs_manufacturing_operators ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_manufacturing_operators ) END   AS tm_nocs_manufacturing_operators
        ,CASE WHEN SUM( m1.nocs_manufacturing_assemblers ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_manufacturing_assemblers ) END AS tm_nocs_manufacturing_assemblers
        ,CASE WHEN SUM( m1.nocs_manufacturing_labourers ) < 0.0001 THEN 0.0001 ELSE SUM( m1.nocs_manufacturing_labourers ) END   AS tm_nocs_manufacturing_labourers
    FROM all_wide_er p0
    LEFT JOIN all_wide_er m1 USING(code)
    WHERE p0.ref_year - m1.ref_year IN ( 1, 2 ) 
    GROUP BY  p0.id
),
all_imputation_input AS(
    SELECT  id
        ,CASE WHEN lbf < 0.0001 THEN 0.0001 ELSE lbf END                                                     AS lbf
        ,CASE WHEN employed < 0.0001 THEN 0.0001 ELSE employed END                                           AS employed
        ,CASE WHEN employed_full < 0.0001 THEN 0.0001 ELSE employed_full END                                 AS employed_full
        ,CASE WHEN employed_part < 0.0001 THEN 0.0001 ELSE employed_part END                                 AS employed_part
        ,CASE WHEN unemployed < 0.0001 THEN 0.0001 ELSE unemployed END                                       AS unemployed
        ,CASE WHEN not_in_lbf < 0.0001 THEN 0.0001 ELSE not_in_lbf END                                       AS not_in_lbf
        ,CASE WHEN naics_employed < 0.0001 THEN 0.0001 ELSE naics_employed END                               AS naics_employed
        ,CASE WHEN naics_goods < 0.0001 THEN 0.0001 ELSE naics_goods END                                     AS naics_goods
        ,CASE WHEN naics_agriculture < 0.0001 THEN 0.0001 ELSE naics_agriculture END                         AS naics_agriculture
        ,CASE WHEN naics_natural_resources < 0.0001 THEN 0.0001 ELSE naics_natural_resources END             AS naics_natural_resources
        ,CASE WHEN naics_ultilities < 0.0001 THEN 0.0001 ELSE naics_ultilities END                           AS naics_ultilities
        ,CASE WHEN naics_construction < 0.0001 THEN 0.0001 ELSE naics_construction END                       AS naics_construction
        ,CASE WHEN naics_manufacturing < 0.0001 THEN 0.0001 ELSE naics_manufacturing END                     AS naics_manufacturing
        ,CASE WHEN naics_services < 0.0001 THEN 0.0001 ELSE naics_services END                               AS naics_services
        ,CASE WHEN naics_retail < 0.0001 THEN 0.0001 ELSE naics_retail END                                   AS naics_retail
        ,CASE WHEN naics_transport_warehousing < 0.0001 THEN 0.0001 ELSE naics_transport_warehousing END     AS naics_transport_warehousing
        ,CASE WHEN naics_financial < 0.0001 THEN 0.0001 ELSE naics_financial END                             AS naics_financial
        ,CASE WHEN naics_professional < 0.0001 THEN 0.0001 ELSE naics_professional END                       AS naics_professional
        ,CASE WHEN naics_support_services < 0.0001 THEN 0.0001 ELSE naics_support_services END               AS naics_support_services
        ,CASE WHEN naics_education_services < 0.0001 THEN 0.0001 ELSE naics_education_services END           AS naics_education_services
        ,CASE WHEN naics_health < 0.0001 THEN 0.0001 ELSE naics_health END                                   AS naics_health
        ,CASE WHEN naics_infor_culture < 0.0001 THEN 0.0001 ELSE naics_infor_culture END                     AS naics_infor_culture
        ,CASE WHEN naics_accomdation_food < 0.0001 THEN 0.0001 ELSE naics_accomdation_food END               AS naics_accomdation_food
        ,CASE WHEN naics_other < 0.0001 THEN 0.0001 ELSE naics_other END                                     AS naics_other
        ,CASE WHEN naics_public_admin < 0.0001 THEN 0.0001 ELSE naics_public_admin END                       AS naics_public_admin
        ,CASE WHEN nocs_employed < 0.0001 THEN 0.0001 ELSE nocs_employed END                                 AS nocs_employed
        ,CASE WHEN nocs_management < 0.0001 THEN 0.0001 ELSE nocs_management END                             AS nocs_management
        ,CASE WHEN nocs_management_senior < 0.0001 THEN 0.0001 ELSE nocs_management_senior END               AS nocs_management_senior
        ,CASE WHEN nocs_management_specialized < 0.0001 THEN 0.0001 ELSE nocs_management_specialized END     AS nocs_management_specialized
        ,CASE WHEN nocs_management_retail < 0.0001 THEN 0.0001 ELSE nocs_management_retail END               AS nocs_management_retail
        ,CASE WHEN nocs_management_trades < 0.0001 THEN 0.0001 ELSE nocs_management_trades END               AS nocs_management_trades
        ,CASE WHEN nocs_business < 0.0001 THEN 0.0001 ELSE nocs_business END                                 AS nocs_business
        ,CASE WHEN nocs_business_pro < 0.0001 THEN 0.0001 ELSE nocs_business_pro END                         AS nocs_business_pro
        ,CASE WHEN nocs_business_admin < 0.0001 THEN 0.0001 ELSE nocs_business_admin END                     AS nocs_business_admin
        ,CASE WHEN nocs_business_finance < 0.0001 THEN 0.0001 ELSE nocs_business_finance END                 AS nocs_business_finance
        ,CASE WHEN nocs_business_office < 0.0001 THEN 0.0001 ELSE nocs_business_office END                   AS nocs_business_office
        ,CASE WHEN nocs_business_distribution < 0.0001 THEN 0.0001 ELSE nocs_business_distribution END       AS nocs_business_distribution
        ,CASE WHEN nocs_sciences < 0.0001 THEN 0.0001 ELSE nocs_sciences END                                 AS nocs_sciences
        ,CASE WHEN nocs_sciences_pro < 0.0001 THEN 0.0001 ELSE nocs_sciences_pro END                         AS nocs_sciences_pro
        ,CASE WHEN nocs_sciences_tech < 0.0001 THEN 0.0001 ELSE nocs_sciences_tech END                       AS nocs_sciences_tech
        ,CASE WHEN nocs_health < 0.0001 THEN 0.0001 ELSE nocs_health END                                     AS nocs_health
        ,CASE WHEN nocs_health_nursing < 0.0001 THEN 0.0001 ELSE nocs_health_nursing END                     AS nocs_health_nursing
        ,CASE WHEN nocs_health_pro < 0.0001 THEN 0.0001 ELSE nocs_health_pro END                             AS nocs_health_pro
        ,CASE WHEN nocs_health_tech < 0.0001 THEN 0.0001 ELSE nocs_health_tech END                           AS nocs_health_tech
        ,CASE WHEN nocs_health_assist < 0.0001 THEN 0.0001 ELSE nocs_health_assist END                       AS nocs_health_assist
        ,CASE WHEN nocs_public < 0.0001 THEN 0.0001 ELSE nocs_public END                                     AS nocs_public
        ,CASE WHEN nocs_public_education < 0.0001 THEN 0.0001 ELSE nocs_public_education END                 AS nocs_public_education
        ,CASE WHEN nocs_public_pro < 0.0001 THEN 0.0001 ELSE nocs_public_pro END                             AS nocs_public_pro
        ,CASE WHEN nocs_public_para < 0.0001 THEN 0.0001 ELSE nocs_public_para END                           AS nocs_public_para
        ,CASE WHEN nocs_public_protection < 0.0001 THEN 0.0001 ELSE nocs_public_protection END               AS nocs_public_protection
        ,CASE WHEN nocs_public_support < 0.0001 THEN 0.0001 ELSE nocs_public_support END                     AS nocs_public_support
        ,CASE WHEN nocs_culture < 0.0001 THEN 0.0001 ELSE nocs_culture END                                   AS nocs_culture
        ,CASE WHEN nocs_culture_pro < 0.0001 THEN 0.0001 ELSE nocs_culture_pro END                           AS nocs_culture_pro
        ,CASE WHEN nocs_culture_tech < 0.0001 THEN 0.0001 ELSE nocs_culture_tech END                         AS nocs_culture_tech
        ,CASE WHEN nocs_sales < 0.0001 THEN 0.0001 ELSE nocs_sales END                                       AS nocs_sales
        ,CASE WHEN nocs_sales_retail < 0.0001 THEN 0.0001 ELSE nocs_sales_retail END                         AS nocs_sales_retail
        ,CASE WHEN nocs_sales_service < 0.0001 THEN 0.0001 ELSE nocs_sales_service END                       AS nocs_sales_service
        ,CASE WHEN nocs_sales_wholesale < 0.0001 THEN 0.0001 ELSE nocs_sales_wholesale END                   AS nocs_sales_wholesale
        ,CASE WHEN nocs_sales_other < 0.0001 THEN 0.0001 ELSE nocs_sales_other END                           AS nocs_sales_other
        ,CASE WHEN nocs_sales_support < 0.0001 THEN 0.0001 ELSE nocs_sales_support END                       AS nocs_sales_support
        ,CASE WHEN nocs_sales_nec < 0.0001 THEN 0.0001 ELSE nocs_sales_nec END                               AS nocs_sales_nec
        ,CASE WHEN nocs_trades < 0.0001 THEN 0.0001 ELSE nocs_trades END                                     AS nocs_trades
        ,CASE WHEN nocs_trades_industrial < 0.0001 THEN 0.0001 ELSE nocs_trades_industrial END               AS nocs_trades_industrial
        ,CASE WHEN nocs_trades_maintenance < 0.0001 THEN 0.0001 ELSE nocs_trades_maintenance END             AS nocs_trades_maintenance
        ,CASE WHEN nocs_trades_other < 0.0001 THEN 0.0001 ELSE nocs_trades_other END                         AS nocs_trades_other
        ,CASE WHEN nocs_trades_heavy < 0.0001 THEN 0.0001 ELSE nocs_trades_heavy END                         AS nocs_trades_heavy
        ,CASE WHEN nocs_trades_helpers < 0.0001 THEN 0.0001 ELSE nocs_trades_helpers END                     AS nocs_trades_helpers
        ,CASE WHEN nocs_agriculture < 0.0001 THEN 0.0001 ELSE nocs_agriculture END                           AS nocs_agriculture
        ,CASE WHEN nocs_agriculture_supervisor < 0.0001 THEN 0.0001 ELSE nocs_agriculture_supervisor END     AS nocs_agriculture_supervisor
        ,CASE WHEN nocs_agriculture_workers < 0.0001 THEN 0.0001 ELSE nocs_agriculture_workers END           AS nocs_agriculture_workers
        ,CASE WHEN nocs_agriculture_harvesting < 0.0001 THEN 0.0001 ELSE nocs_agriculture_harvesting END     AS nocs_agriculture_harvesting
        ,CASE WHEN nocs_manufacturing < 0.0001 THEN 0.0001 ELSE nocs_manufacturing END                       AS nocs_manufacturing
        ,CASE WHEN nocs_manufacturing_supervisor < 0.0001 THEN 0.0001 ELSE nocs_manufacturing_supervisor END AS nocs_manufacturing_supervisor
        ,CASE WHEN nocs_manufacturing_operators < 0.0001 THEN 0.0001 ELSE nocs_manufacturing_operators END   AS nocs_manufacturing_operators
        ,CASE WHEN nocs_manufacturing_assemblers < 0.0001 THEN 0.0001 ELSE nocs_manufacturing_assemblers END AS nocs_manufacturing_assemblers
        ,CASE WHEN nocs_manufacturing_labourers < 0.0001 THEN 0.0001 ELSE nocs_manufacturing_labourers END   AS nocs_manufacturing_labourers
        ,tm_lbf
        ,tm_employed
        ,tm_employed_full
        ,tm_employed_part
        ,tm_unemployed
        ,tm_not_in_lbf
        ,tm_naics_employed
        ,tm_naics_goods
        ,tm_naics_agriculture
        ,tm_naics_natural_resources
        ,tm_naics_ultilities
        ,tm_naics_construction
        ,tm_naics_manufacturing
        ,tm_naics_services
        ,tm_naics_retail
        ,tm_naics_transport_warehousing
        ,tm_naics_financial
        ,tm_naics_professional
        ,tm_naics_support_services
        ,tm_naics_education_services
        ,tm_naics_health
        ,tm_naics_infor_culture
        ,tm_naics_accomdation_food
        ,tm_naics_other
        ,tm_naics_public_admin
        ,tm_nocs_employed
        ,tm_nocs_management
        ,tm_nocs_management_senior
        ,tm_nocs_management_specialized
        ,tm_nocs_management_retail
        ,tm_nocs_management_trades
        ,tm_nocs_business
        ,tm_nocs_business_pro
        ,tm_nocs_business_admin
        ,tm_nocs_business_finance
        ,tm_nocs_business_office
        ,tm_nocs_business_distribution
        ,tm_nocs_sciences
        ,tm_nocs_sciences_pro
        ,tm_nocs_sciences_tech
        ,tm_nocs_health
        ,tm_nocs_health_nursing
        ,tm_nocs_health_pro
        ,tm_nocs_health_tech
        ,tm_nocs_health_assist
        ,tm_nocs_public
        ,tm_nocs_public_education
        ,tm_nocs_public_pro
        ,tm_nocs_public_para
        ,tm_nocs_public_protection
        ,tm_nocs_public_support
        ,tm_nocs_culture
        ,tm_nocs_culture_pro
        ,tm_nocs_culture_tech
        ,tm_nocs_sales
        ,tm_nocs_sales_retail
        ,tm_nocs_sales_service
        ,tm_nocs_sales_wholesale
        ,tm_nocs_sales_other
        ,tm_nocs_sales_support
        ,tm_nocs_sales_nec
        ,tm_nocs_trades
        ,tm_nocs_trades_industrial
        ,tm_nocs_trades_maintenance
        ,tm_nocs_trades_other
        ,tm_nocs_trades_heavy
        ,tm_nocs_trades_helpers
        ,tm_nocs_agriculture
        ,tm_nocs_agriculture_supervisor
        ,tm_nocs_agriculture_workers
        ,tm_nocs_agriculture_harvesting
        ,tm_nocs_manufacturing
        ,tm_nocs_manufacturing_supervisor
        ,tm_nocs_manufacturing_operators
        ,tm_nocs_manufacturing_assemblers
        ,tm_nocs_manufacturing_labourers
    FROM all_wide_er A
    INNER JOIN all_wide_t_minus b USING (id)
)
SELECT  *
FROM all_imputation_input
