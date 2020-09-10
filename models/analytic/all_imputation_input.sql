WITH all_wide_er AS (
    SELECT  *
    FROM {{ ref('all_wide_raw') }}
    WHERE geo_level_name = 'Economic Region' 
),
all_wide_t_minus AS (
    SELECT  p0.id 
       ,SUM( m1.lbf ) / 2                           AS tm_lbf 
       ,SUM( m1.employed ) / 2                      AS tm_employed 
       ,SUM( m1.employed_full ) / 2                 AS tm_employed_full 
       ,SUM( m1.employed_part ) / 2                 AS tm_employed_part 
       ,SUM( m1.unemployed ) / 2                    AS tm_unemployed 
       ,SUM( m1.not_in_lbf ) / 2                    AS tm_not_in_lbf 
       ,SUM( m1.naics_employed ) / 2                AS tm_naics_employed 
       ,SUM( m1.naics_goods ) / 2                   AS tm_naics_goods 
       ,SUM( m1.naics_agriculture ) / 2             AS tm_naics_agriculture 
       ,SUM( m1.naics_natural_resources ) / 2       AS tm_naics_natural_resources 
       ,SUM( m1.naics_ultilities ) / 2              AS tm_naics_ultilities 
       ,SUM( m1.naics_construction ) / 2            AS tm_naics_construction 
       ,SUM( m1.naics_manufacturing ) / 2           AS tm_naics_manufacturing 
       ,SUM( m1.naics_services ) / 2                AS tm_naics_services 
       ,SUM( m1.naics_retail ) / 2                  AS tm_naics_retail 
       ,SUM( m1.naics_transport_warehousing ) / 2   AS tm_naics_transport_warehousing 
       ,SUM( m1.naics_financial ) / 2               AS tm_naics_financial 
       ,SUM( m1.naics_professional ) / 2            AS tm_naics_professional 
       ,SUM( m1.naics_support_services ) / 2        AS tm_naics_support_services 
       ,SUM( m1.naics_education_services ) / 2      AS tm_naics_education_services 
       ,SUM( m1.naics_health ) / 2                  AS tm_naics_health 
       ,SUM( m1.naics_infor_culture ) / 2           AS tm_naics_infor_culture 
       ,SUM( m1.naics_accomdation_food ) / 2        AS tm_naics_accomdation_food 
       ,SUM( m1.naics_other ) / 2                   AS tm_naics_other 
       ,SUM( m1.naics_public_admin ) / 2            AS tm_naics_public_admin 
       ,SUM( m1.nocs_employed ) / 2                 AS tm_nocs_employed 
       ,SUM( m1.nocs_management ) / 2               AS tm_nocs_management 
       ,SUM( m1.nocs_management_senior ) / 2        AS tm_nocs_management_senior 
       ,SUM( m1.nocs_management_specialized ) / 2   AS tm_nocs_management_specialized 
       ,SUM( m1.nocs_management_retail ) / 2        AS tm_nocs_management_retail 
       ,SUM( m1.nocs_management_trades ) / 2        AS tm_nocs_management_trades 
       ,SUM( m1.nocs_business ) / 2                 AS tm_nocs_business 
       ,SUM( m1.nocs_business_pro ) / 2             AS tm_nocs_business_pro 
       ,SUM( m1.nocs_business_admin ) / 2           AS tm_nocs_business_admin 
       ,SUM( m1.nocs_business_finance ) / 2         AS tm_nocs_business_finance 
       ,SUM( m1.nocs_business_office ) / 2          AS tm_nocs_business_office 
       ,SUM( m1.nocs_business_distribution ) / 2    AS tm_nocs_business_distribution 
       ,SUM( m1.nocs_sciences ) / 2                 AS tm_nocs_sciences 
       ,SUM( m1.nocs_sciences_pro ) / 2             AS tm_nocs_sciences_pro 
       ,SUM( m1.nocs_sciences_tech ) / 2            AS tm_nocs_sciences_tech 
       ,SUM( m1.nocs_health ) / 2                   AS tm_nocs_health 
       ,SUM( m1.nocs_health_nursing ) / 2           AS tm_nocs_health_nursing 
       ,SUM( m1.nocs_health_pro ) / 2               AS tm_nocs_health_pro 
       ,SUM( m1.nocs_health_tech ) / 2              AS tm_nocs_health_tech 
       ,SUM( m1.nocs_health_assist ) / 2            AS tm_nocs_health_assist 
       ,SUM( m1.nocs_public ) / 2                   AS tm_nocs_public 
       ,SUM( m1.nocs_public_education ) / 2         AS tm_nocs_public_education 
       ,SUM( m1.nocs_public_pro ) / 2               AS tm_nocs_public_pro 
       ,SUM( m1.nocs_public_para ) / 2              AS tm_nocs_public_para 
       ,SUM( m1.nocs_public_protection ) / 2        AS tm_nocs_public_protection 
       ,SUM( m1.nocs_public_support ) / 2           AS tm_nocs_public_support 
       ,SUM( m1.nocs_culture ) / 2                  AS tm_nocs_culture 
       ,SUM( m1.nocs_culture_pro ) / 2              AS tm_nocs_culture_pro 
       ,SUM( m1.nocs_culture_tech ) / 2             AS tm_nocs_culture_tech 
       ,SUM( m1.nocs_sales ) / 2                    AS tm_nocs_sales 
       ,SUM( m1.nocs_sales_retail ) / 2             AS tm_nocs_sales_retail 
       ,SUM( m1.nocs_sales_service ) / 2            AS tm_nocs_sales_service 
       ,SUM( m1.nocs_sales_wholesale ) / 2          AS tm_nocs_sales_wholesale 
       ,SUM( m1.nocs_sales_other ) / 2              AS tm_nocs_sales_other 
       ,SUM( m1.nocs_sales_support ) / 2            AS tm_nocs_sales_support 
       ,SUM( m1.nocs_sales_nec ) / 2                AS tm_nocs_sales_nec 
       ,SUM( m1.nocs_trades ) / 2                   AS tm_nocs_trades 
       ,SUM( m1.nocs_trades_industrial ) / 2        AS tm_nocs_trades_industrial 
       ,SUM( m1.nocs_trades_maintenance ) / 2       AS tm_nocs_trades_maintenance 
       ,SUM( m1.nocs_trades_other ) / 2             AS tm_nocs_trades_other 
       ,SUM( m1.nocs_trades_heavy ) / 2             AS tm_nocs_trades_heavy 
       ,SUM( m1.nocs_trades_helpers ) / 2           AS tm_nocs_trades_helpers 
       ,SUM( m1.nocs_agriculture ) / 2              AS tm_nocs_agriculture 
       ,SUM( m1.nocs_agriculture_supervisor ) / 2   AS tm_nocs_agriculture_supervisor 
       ,SUM( m1.nocs_agriculture_workers ) / 2      AS tm_nocs_agriculture_workers 
       ,SUM( m1.nocs_agriculture_harvesting ) / 2   AS tm_nocs_agriculture_harvesting 
       ,SUM( m1.nocs_manufacturing ) / 2            AS tm_nocs_manufacturing 
       ,SUM( m1.nocs_manufacturing_supervisor ) / 2 AS tm_nocs_manufacturing_supervisor 
       ,SUM( m1.nocs_manufacturing_operators ) / 2  AS tm_nocs_manufacturing_operators 
       ,SUM( m1.nocs_manufacturing_assemblers ) / 2 AS tm_nocs_manufacturing_assemblers 
       ,SUM( m1.nocs_manufacturing_labourers ) / 2  AS tm_nocs_manufacturing_labourers
    FROM all_wide_er p0
    LEFT JOIN all_wide_er m1 USING (code)
    WHERE p0.ref_year - m1.ref_year IN ( 1, 2 ) OR (p0.ref_year = 2001 AND m1.ref_year IN (2002, 2003)) 
    GROUP BY  p0.id
),
all_imputation_input AS (
    SELECT a.id
		,CASE WHEN c.lbf = 1 THEN 0.0001 ELSE a.lbf END                                                     AS lbf
        ,CASE WHEN c.employed = 1 THEN 0.0001 ELSE a.employed END                                           AS employed
        ,CASE WHEN c.employed_full = 1 THEN 0.0001 ELSE a.employed_full END                                 AS employed_full
        ,CASE WHEN c.employed_part = 1 THEN 0.0001 ELSE a.employed_part END                                 AS employed_part
        ,CASE WHEN c.unemployed = 1 THEN 0.0001 ELSE a.unemployed END                                       AS unemployed
        ,CASE WHEN c.not_in_lbf = 1 THEN 0.0001 ELSE a.not_in_lbf END                                       AS not_in_lbf
        ,CASE WHEN c.naics_employed = 1 THEN 0.0001 ELSE a.naics_employed END                               AS naics_employed
        ,CASE WHEN c.naics_goods = 1 THEN 0.0001 ELSE a.naics_goods END                                     AS naics_goods
        ,CASE WHEN c.naics_agriculture = 1 THEN 0.0001 ELSE a.naics_agriculture END                         AS naics_agriculture
        ,CASE WHEN c.naics_natural_resources = 1 THEN 0.0001 ELSE a.naics_natural_resources END             AS naics_natural_resources
        ,CASE WHEN c.naics_ultilities = 1 THEN 0.0001 ELSE a.naics_ultilities END                           AS naics_ultilities
        ,CASE WHEN c.naics_construction = 1 THEN 0.0001 ELSE a.naics_construction END                       AS naics_construction
        ,CASE WHEN c.naics_manufacturing = 1 THEN 0.0001 ELSE a.naics_manufacturing END                     AS naics_manufacturing
        ,CASE WHEN c.naics_services = 1 THEN 0.0001 ELSE a.naics_services END                               AS naics_services
        ,CASE WHEN c.naics_retail = 1 THEN 0.0001 ELSE a.naics_retail END                                   AS naics_retail
        ,CASE WHEN c.naics_transport_warehousing = 1 THEN 0.0001 ELSE a.naics_transport_warehousing END     AS naics_transport_warehousing
        ,CASE WHEN c.naics_financial = 1 THEN 0.0001 ELSE a.naics_financial END                             AS naics_financial
        ,CASE WHEN c.naics_professional = 1 THEN 0.0001 ELSE a.naics_professional END                       AS naics_professional
        ,CASE WHEN c.naics_support_services = 1 THEN 0.0001 ELSE a.naics_support_services END               AS naics_support_services
        ,CASE WHEN c.naics_education_services = 1 THEN 0.0001 ELSE a.naics_education_services END           AS naics_education_services
        ,CASE WHEN c.naics_health = 1 THEN 0.0001 ELSE a.naics_health END                                   AS naics_health
        ,CASE WHEN c.naics_infor_culture = 1 THEN 0.0001 ELSE a.naics_infor_culture END                     AS naics_infor_culture
        ,CASE WHEN c.naics_accomdation_food = 1 THEN 0.0001 ELSE a.naics_accomdation_food END               AS naics_accomdation_food
        ,CASE WHEN c.naics_other = 1 THEN 0.0001 ELSE a.naics_other END                                     AS naics_other
        ,CASE WHEN c.naics_public_admin = 1 THEN 0.0001 ELSE a.naics_public_admin END                       AS naics_public_admin
        ,CASE WHEN c.nocs_employed = 1 THEN 0.0001 ELSE a.nocs_employed END                                 AS nocs_employed
        ,CASE WHEN c.nocs_management = 1 THEN 0.0001 ELSE a.nocs_management END                             AS nocs_management
        ,CASE WHEN c.nocs_management_senior = 1 THEN 0.0001 ELSE a.nocs_management_senior END               AS nocs_management_senior
        ,CASE WHEN c.nocs_management_specialized = 1 THEN 0.0001 ELSE a.nocs_management_specialized END     AS nocs_management_specialized
        ,CASE WHEN c.nocs_management_retail = 1 THEN 0.0001 ELSE a.nocs_management_retail END               AS nocs_management_retail
        ,CASE WHEN c.nocs_management_trades = 1 THEN 0.0001 ELSE a.nocs_management_trades END               AS nocs_management_trades
        ,CASE WHEN c.nocs_business = 1 THEN 0.0001 ELSE a.nocs_business END                                 AS nocs_business
        ,CASE WHEN c.nocs_business_pro = 1 THEN 0.0001 ELSE a.nocs_business_pro END                         AS nocs_business_pro
        ,CASE WHEN c.nocs_business_admin = 1 THEN 0.0001 ELSE a.nocs_business_admin END                     AS nocs_business_admin
        ,CASE WHEN c.nocs_business_finance = 1 THEN 0.0001 ELSE a.nocs_business_finance END                 AS nocs_business_finance
        ,CASE WHEN c.nocs_business_office = 1 THEN 0.0001 ELSE a.nocs_business_office END                   AS nocs_business_office
        ,CASE WHEN c.nocs_business_distribution = 1 THEN 0.0001 ELSE a.nocs_business_distribution END       AS nocs_business_distribution
        ,CASE WHEN c.nocs_sciences = 1 THEN 0.0001 ELSE a.nocs_sciences END                                 AS nocs_sciences
        ,CASE WHEN c.nocs_sciences_pro = 1 THEN 0.0001 ELSE a.nocs_sciences_pro END                         AS nocs_sciences_pro
        ,CASE WHEN c.nocs_sciences_tech = 1 THEN 0.0001 ELSE a.nocs_sciences_tech END                       AS nocs_sciences_tech
        ,CASE WHEN c.nocs_health = 1 THEN 0.0001 ELSE a.nocs_health END                                     AS nocs_health
        ,CASE WHEN c.nocs_health_nursing = 1 THEN 0.0001 ELSE a.nocs_health_nursing END                     AS nocs_health_nursing
        ,CASE WHEN c.nocs_health_pro = 1 THEN 0.0001 ELSE a.nocs_health_pro END                             AS nocs_health_pro
        ,CASE WHEN c.nocs_health_tech = 1 THEN 0.0001 ELSE a.nocs_health_tech END                           AS nocs_health_tech
        ,CASE WHEN c.nocs_health_assist = 1 THEN 0.0001 ELSE a.nocs_health_assist END                       AS nocs_health_assist
        ,CASE WHEN c.nocs_public = 1 THEN 0.0001 ELSE a.nocs_public END                                     AS nocs_public
        ,CASE WHEN c.nocs_public_education = 1 THEN 0.0001 ELSE a.nocs_public_education END                 AS nocs_public_education
        ,CASE WHEN c.nocs_public_pro = 1 THEN 0.0001 ELSE a.nocs_public_pro END                             AS nocs_public_pro
        ,CASE WHEN c.nocs_public_para = 1 THEN 0.0001 ELSE a.nocs_public_para END                           AS nocs_public_para
        ,CASE WHEN c.nocs_public_protection = 1 THEN 0.0001 ELSE a.nocs_public_protection END               AS nocs_public_protection
        ,CASE WHEN c.nocs_public_support = 1 THEN 0.0001 ELSE a.nocs_public_support END                     AS nocs_public_support
        ,CASE WHEN c.nocs_culture = 1 THEN 0.0001 ELSE a.nocs_culture END                                   AS nocs_culture
        ,CASE WHEN c.nocs_culture_pro = 1 THEN 0.0001 ELSE a.nocs_culture_pro END                           AS nocs_culture_pro
        ,CASE WHEN c.nocs_culture_tech = 1 THEN 0.0001 ELSE a.nocs_culture_tech END                         AS nocs_culture_tech
        ,CASE WHEN c.nocs_sales = 1 THEN 0.0001 ELSE a.nocs_sales END                                       AS nocs_sales
        ,CASE WHEN c.nocs_sales_retail = 1 THEN 0.0001 ELSE a.nocs_sales_retail END                         AS nocs_sales_retail
        ,CASE WHEN c.nocs_sales_service = 1 THEN 0.0001 ELSE a.nocs_sales_service END                       AS nocs_sales_service
        ,CASE WHEN c.nocs_sales_wholesale = 1 THEN 0.0001 ELSE a.nocs_sales_wholesale END                   AS nocs_sales_wholesale
        ,CASE WHEN c.nocs_sales_other = 1 THEN 0.0001 ELSE a.nocs_sales_other END                           AS nocs_sales_other
        ,CASE WHEN c.nocs_sales_support = 1 THEN 0.0001 ELSE a.nocs_sales_support END                       AS nocs_sales_support
        ,CASE WHEN c.nocs_sales_nec = 1 THEN 0.0001 ELSE a.nocs_sales_nec END                               AS nocs_sales_nec
        ,CASE WHEN c.nocs_trades = 1 THEN 0.0001 ELSE a.nocs_trades END                                     AS nocs_trades
        ,CASE WHEN c.nocs_trades_industrial = 1 THEN 0.0001 ELSE a.nocs_trades_industrial END               AS nocs_trades_industrial
        ,CASE WHEN c.nocs_trades_maintenance = 1 THEN 0.0001 ELSE a.nocs_trades_maintenance END             AS nocs_trades_maintenance
        ,CASE WHEN c.nocs_trades_other = 1 THEN 0.0001 ELSE a.nocs_trades_other END                         AS nocs_trades_other
        ,CASE WHEN c.nocs_trades_heavy = 1 THEN 0.0001 ELSE a.nocs_trades_heavy END                         AS nocs_trades_heavy
        ,CASE WHEN c.nocs_trades_helpers = 1 THEN 0.0001 ELSE a.nocs_trades_helpers END                     AS nocs_trades_helpers
        ,CASE WHEN c.nocs_agriculture = 1 THEN 0.0001 ELSE a.nocs_agriculture END                           AS nocs_agriculture
        ,CASE WHEN c.nocs_agriculture_supervisor = 1 THEN 0.0001 ELSE a.nocs_agriculture_supervisor END     AS nocs_agriculture_supervisor
        ,CASE WHEN c.nocs_agriculture_workers = 1 THEN 0.0001 ELSE a.nocs_agriculture_workers END           AS nocs_agriculture_workers
        ,CASE WHEN c.nocs_agriculture_harvesting = 1 THEN 0.0001 ELSE a.nocs_agriculture_harvesting END     AS nocs_agriculture_harvesting
        ,CASE WHEN c.nocs_manufacturing = 1 THEN 0.0001 ELSE a.nocs_manufacturing END                       AS nocs_manufacturing
        ,CASE WHEN c.nocs_manufacturing_supervisor = 1 THEN 0.0001 ELSE a.nocs_manufacturing_supervisor END AS nocs_manufacturing_supervisor
        ,CASE WHEN c.nocs_manufacturing_operators = 1 THEN 0.0001 ELSE a.nocs_manufacturing_operators END   AS nocs_manufacturing_operators
        ,CASE WHEN c.nocs_manufacturing_assemblers = 1 THEN 0.0001 ELSE a.nocs_manufacturing_assemblers END AS nocs_manufacturing_assemblers
        ,CASE WHEN c.nocs_manufacturing_labourers = 1 THEN 0.0001 ELSE a.nocs_manufacturing_labourers END   AS nocs_manufacturing_labourers
        ,CASE WHEN c.lbf = 1 THEN 0.0001 ELSE b.tm_lbf END                                                     AS tm_lbf
        ,CASE WHEN c.employed = 1 THEN 0.0001 ELSE b.tm_employed END                                           AS tm_employed
        ,CASE WHEN c.employed_full = 1 THEN 0.0001 ELSE b.tm_employed_full END                                 AS tm_employed_full
        ,CASE WHEN c.employed_part = 1 THEN 0.0001 ELSE b.tm_employed_part END                                 AS tm_employed_part
        ,CASE WHEN c.unemployed = 1 THEN 0.0001 ELSE b.tm_unemployed END                                       AS tm_unemployed
        ,CASE WHEN c.not_in_lbf = 1 THEN 0.0001 ELSE b.tm_not_in_lbf END                                       AS tm_not_in_lbf
        ,CASE WHEN c.naics_employed = 1 THEN 0.0001 ELSE b.tm_naics_employed END                               AS tm_naics_employed
        ,CASE WHEN c.naics_goods = 1 THEN 0.0001 ELSE b.tm_naics_goods END                                     AS tm_naics_goods
        ,CASE WHEN c.naics_agriculture = 1 THEN 0.0001 ELSE b.tm_naics_agriculture END                         AS tm_naics_agriculture
        ,CASE WHEN c.naics_natural_resources = 1 THEN 0.0001 ELSE b.tm_naics_natural_resources END             AS tm_naics_natural_resources
        ,CASE WHEN c.naics_ultilities = 1 THEN 0.0001 ELSE b.tm_naics_ultilities END                           AS tm_naics_ultilities
        ,CASE WHEN c.naics_construction = 1 THEN 0.0001 ELSE b.tm_naics_construction END                       AS tm_naics_construction
        ,CASE WHEN c.naics_manufacturing = 1 THEN 0.0001 ELSE b.tm_naics_manufacturing END                     AS tm_naics_manufacturing
        ,CASE WHEN c.naics_services = 1 THEN 0.0001 ELSE b.tm_naics_services END                               AS tm_naics_services
        ,CASE WHEN c.naics_retail = 1 THEN 0.0001 ELSE b.tm_naics_retail END                                   AS tm_naics_retail
        ,CASE WHEN c.naics_transport_warehousing = 1 THEN 0.0001 ELSE b.tm_naics_transport_warehousing END     AS tm_naics_transport_warehousing
        ,CASE WHEN c.naics_financial = 1 THEN 0.0001 ELSE b.tm_naics_financial END                             AS tm_naics_financial
        ,CASE WHEN c.naics_professional = 1 THEN 0.0001 ELSE b.tm_naics_professional END                       AS tm_naics_professional
        ,CASE WHEN c.naics_support_services = 1 THEN 0.0001 ELSE b.tm_naics_support_services END               AS tm_naics_support_services
        ,CASE WHEN c.naics_education_services = 1 THEN 0.0001 ELSE b.tm_naics_education_services END           AS tm_naics_education_services
        ,CASE WHEN c.naics_health = 1 THEN 0.0001 ELSE b.tm_naics_health END                                   AS tm_naics_health
        ,CASE WHEN c.naics_infor_culture = 1 THEN 0.0001 ELSE b.tm_naics_infor_culture END                     AS tm_naics_infor_culture
        ,CASE WHEN c.naics_accomdation_food = 1 THEN 0.0001 ELSE b.tm_naics_accomdation_food END               AS tm_naics_accomdation_food
        ,CASE WHEN c.naics_other = 1 THEN 0.0001 ELSE b.tm_naics_other END                                     AS tm_naics_other
        ,CASE WHEN c.naics_public_admin = 1 THEN 0.0001 ELSE b.tm_naics_public_admin END                       AS tm_naics_public_admin
        ,CASE WHEN c.nocs_employed = 1 THEN 0.0001 ELSE b.tm_nocs_employed END                                 AS tm_nocs_employed
        ,CASE WHEN c.nocs_management = 1 THEN 0.0001 ELSE b.tm_nocs_management END                             AS tm_nocs_management
        ,CASE WHEN c.nocs_management_senior = 1 THEN 0.0001 ELSE b.tm_nocs_management_senior END               AS tm_nocs_management_senior
        ,CASE WHEN c.nocs_management_specialized = 1 THEN 0.0001 ELSE b.tm_nocs_management_specialized END     AS tm_nocs_management_specialized
        ,CASE WHEN c.nocs_management_retail = 1 THEN 0.0001 ELSE b.tm_nocs_management_retail END               AS tm_nocs_management_retail
        ,CASE WHEN c.nocs_management_trades = 1 THEN 0.0001 ELSE b.tm_nocs_management_trades END               AS tm_nocs_management_trades
        ,CASE WHEN c.nocs_business = 1 THEN 0.0001 ELSE b.tm_nocs_business END                                 AS tm_nocs_business
        ,CASE WHEN c.nocs_business_pro = 1 THEN 0.0001 ELSE b.tm_nocs_business_pro END                         AS tm_nocs_business_pro
        ,CASE WHEN c.nocs_business_admin = 1 THEN 0.0001 ELSE b.tm_nocs_business_admin END                     AS tm_nocs_business_admin
        ,CASE WHEN c.nocs_business_finance = 1 THEN 0.0001 ELSE b.tm_nocs_business_finance END                 AS tm_nocs_business_finance
        ,CASE WHEN c.nocs_business_office = 1 THEN 0.0001 ELSE b.tm_nocs_business_office END                   AS tm_nocs_business_office
        ,CASE WHEN c.nocs_business_distribution = 1 THEN 0.0001 ELSE b.tm_nocs_business_distribution END       AS tm_nocs_business_distribution
        ,CASE WHEN c.nocs_sciences = 1 THEN 0.0001 ELSE b.tm_nocs_sciences END                                 AS tm_nocs_sciences
        ,CASE WHEN c.nocs_sciences_pro = 1 THEN 0.0001 ELSE b.tm_nocs_sciences_pro END                         AS tm_nocs_sciences_pro
        ,CASE WHEN c.nocs_sciences_tech = 1 THEN 0.0001 ELSE b.tm_nocs_sciences_tech END                       AS tm_nocs_sciences_tech
        ,CASE WHEN c.nocs_health = 1 THEN 0.0001 ELSE b.tm_nocs_health END                                     AS tm_nocs_health
        ,CASE WHEN c.nocs_health_nursing = 1 THEN 0.0001 ELSE b.tm_nocs_health_nursing END                     AS tm_nocs_health_nursing
        ,CASE WHEN c.nocs_health_pro = 1 THEN 0.0001 ELSE b.tm_nocs_health_pro END                             AS tm_nocs_health_pro
        ,CASE WHEN c.nocs_health_tech = 1 THEN 0.0001 ELSE b.tm_nocs_health_tech END                           AS tm_nocs_health_tech
        ,CASE WHEN c.nocs_health_assist = 1 THEN 0.0001 ELSE b.tm_nocs_health_assist END                       AS tm_nocs_health_assist
        ,CASE WHEN c.nocs_public = 1 THEN 0.0001 ELSE b.tm_nocs_public END                                     AS tm_nocs_public
        ,CASE WHEN c.nocs_public_education = 1 THEN 0.0001 ELSE b.tm_nocs_public_education END                 AS tm_nocs_public_education
        ,CASE WHEN c.nocs_public_pro = 1 THEN 0.0001 ELSE b.tm_nocs_public_pro END                             AS tm_nocs_public_pro
        ,CASE WHEN c.nocs_public_para = 1 THEN 0.0001 ELSE b.tm_nocs_public_para END                           AS tm_nocs_public_para
        ,CASE WHEN c.nocs_public_protection = 1 THEN 0.0001 ELSE b.tm_nocs_public_protection END               AS tm_nocs_public_protection
        ,CASE WHEN c.nocs_public_support = 1 THEN 0.0001 ELSE b.tm_nocs_public_support END                     AS tm_nocs_public_support
        ,CASE WHEN c.nocs_culture = 1 THEN 0.0001 ELSE b.tm_nocs_culture END                                   AS tm_nocs_culture
        ,CASE WHEN c.nocs_culture_pro = 1 THEN 0.0001 ELSE b.tm_nocs_culture_pro END                           AS tm_nocs_culture_pro
        ,CASE WHEN c.nocs_culture_tech = 1 THEN 0.0001 ELSE b.tm_nocs_culture_tech END                         AS tm_nocs_culture_tech
        ,CASE WHEN c.nocs_sales = 1 THEN 0.0001 ELSE b.tm_nocs_sales END                                       AS tm_nocs_sales
        ,CASE WHEN c.nocs_sales_retail = 1 THEN 0.0001 ELSE b.tm_nocs_sales_retail END                         AS tm_nocs_sales_retail
        ,CASE WHEN c.nocs_sales_service = 1 THEN 0.0001 ELSE b.tm_nocs_sales_service END                       AS tm_nocs_sales_service
        ,CASE WHEN c.nocs_sales_wholesale = 1 THEN 0.0001 ELSE b.tm_nocs_sales_wholesale END                   AS tm_nocs_sales_wholesale
        ,CASE WHEN c.nocs_sales_other = 1 THEN 0.0001 ELSE b.tm_nocs_sales_other END                           AS tm_nocs_sales_other
        ,CASE WHEN c.nocs_sales_support = 1 THEN 0.0001 ELSE b.tm_nocs_sales_support END                       AS tm_nocs_sales_support
        ,CASE WHEN c.nocs_sales_nec = 1 THEN 0.0001 ELSE b.tm_nocs_sales_nec END                               AS tm_nocs_sales_nec
        ,CASE WHEN c.nocs_trades = 1 THEN 0.0001 ELSE b.tm_nocs_trades END                                     AS tm_nocs_trades
        ,CASE WHEN c.nocs_trades_industrial = 1 THEN 0.0001 ELSE b.tm_nocs_trades_industrial END               AS tm_nocs_trades_industrial
        ,CASE WHEN c.nocs_trades_maintenance = 1 THEN 0.0001 ELSE b.tm_nocs_trades_maintenance END             AS tm_nocs_trades_maintenance
        ,CASE WHEN c.nocs_trades_other = 1 THEN 0.0001 ELSE b.tm_nocs_trades_other END                         AS tm_nocs_trades_other
        ,CASE WHEN c.nocs_trades_heavy = 1 THEN 0.0001 ELSE b.tm_nocs_trades_heavy END                         AS tm_nocs_trades_heavy
        ,CASE WHEN c.nocs_trades_helpers = 1 THEN 0.0001 ELSE b.tm_nocs_trades_helpers END                     AS tm_nocs_trades_helpers
        ,CASE WHEN c.nocs_agriculture = 1 THEN 0.0001 ELSE b.tm_nocs_agriculture END                           AS tm_nocs_agriculture
        ,CASE WHEN c.nocs_agriculture_supervisor = 1 THEN 0.0001 ELSE b.tm_nocs_agriculture_supervisor END     AS tm_nocs_agriculture_supervisor
        ,CASE WHEN c.nocs_agriculture_workers = 1 THEN 0.0001 ELSE b.tm_nocs_agriculture_workers END           AS tm_nocs_agriculture_workers
        ,CASE WHEN c.nocs_agriculture_harvesting = 1 THEN 0.0001 ELSE b.tm_nocs_agriculture_harvesting END     AS tm_nocs_agriculture_harvesting
        ,CASE WHEN c.nocs_manufacturing = 1 THEN 0.0001 ELSE b.tm_nocs_manufacturing END                       AS tm_nocs_manufacturing
        ,CASE WHEN c.nocs_manufacturing_supervisor = 1 THEN 0.0001 ELSE b.tm_nocs_manufacturing_supervisor END AS tm_nocs_manufacturing_supervisor
        ,CASE WHEN c.nocs_manufacturing_operators = 1 THEN 0.0001 ELSE b.tm_nocs_manufacturing_operators END   AS tm_nocs_manufacturing_operators
        ,CASE WHEN c.nocs_manufacturing_assemblers = 1 THEN 0.0001 ELSE b.tm_nocs_manufacturing_assemblers END AS tm_nocs_manufacturing_assemblers
        ,CASE WHEN c.nocs_manufacturing_labourers = 1 THEN 0.0001 ELSE b.tm_nocs_manufacturing_labourers END   AS tm_nocs_manufacturing_labourers
    FROM all_wide_er AS a
    INNER JOIN all_wide_t_minus AS b USING (id)
    INNER JOIN {{ ref('all_suppression') }} AS c USING (id)
)
SELECT * FROM all_imputation_input
