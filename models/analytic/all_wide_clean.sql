WITH apply_imputes AS (
    SELECT  rw.id 
       ,rw.code
       ,rw.ref_year 
       ,rw.geo_name 
       ,rw.geo_parent_name 
       ,rw.pop15p 
       ,CASE WHEN sp.lbf = 1 THEN ii.lbf ELSE rw.lbf END                                                                               AS lbf 
       ,CASE WHEN sp.employed = 1 THEN ii.employed ELSE rw.employed END                                                                AS employed 
       ,CASE WHEN sp.employed_full = 1 THEN ii.employed_full ELSE rw.employed_full END                                                 AS employed_full 
       ,CASE WHEN sp.employed_part = 1 THEN ii.employed_part ELSE rw.employed_part END                                                 AS employed_part 
       ,CASE WHEN sp.unemployed = 1 THEN ii.unemployed ELSE rw.unemployed END                                                          AS unemployed 
       ,CASE WHEN sp.not_in_lbf = 1 THEN ii.not_in_lbf ELSE rw.not_in_lbf END                                                          AS not_in_lbf 
       ,CASE WHEN sp.naics_employed = 1 THEN ii.naics_employed ELSE rw.naics_employed END                                              AS naics_employed 
       ,CASE WHEN sp.naics_goods = 1 THEN ii.naics_goods ELSE rw.naics_goods END                                                       AS naics_goods 
       ,CASE WHEN sp.naics_agriculture = 1 THEN ii.naics_agriculture ELSE rw.naics_agriculture END                                     AS naics_agriculture 
       ,CASE WHEN sp.naics_natural_resources = 1 THEN ii.naics_natural_resources ELSE rw.naics_natural_resources END                   AS naics_natural_resources 
       ,CASE WHEN sp.naics_ultilities = 1 THEN ii.naics_ultilities ELSE rw.naics_ultilities END                                        AS naics_ultilities 
       ,CASE WHEN sp.naics_construction = 1 THEN ii.naics_construction ELSE rw.naics_construction END                                  AS naics_construction 
       ,CASE WHEN sp.naics_manufacturing = 1 THEN ii.naics_manufacturing ELSE rw.naics_manufacturing END                               AS naics_manufacturing 
       ,CASE WHEN sp.naics_services = 1 THEN ii.naics_services ELSE rw.naics_services END                                              AS naics_services 
       ,CASE WHEN sp.naics_retail = 1 THEN ii.naics_retail ELSE rw.naics_retail END                                                    AS naics_retail 
       ,CASE WHEN sp.naics_transport_warehousing = 1 THEN ii.naics_transport_warehousing ELSE rw.naics_transport_warehousing END       AS naics_transport_warehousing 
       ,CASE WHEN sp.naics_financial = 1 THEN ii.naics_financial ELSE rw.naics_financial END                                           AS naics_financial 
       ,CASE WHEN sp.naics_professional = 1 THEN ii.naics_professional ELSE rw.naics_professional END                                  AS naics_professional 
       ,CASE WHEN sp.naics_support_services = 1 THEN ii.naics_support_services ELSE rw.naics_support_services END                      AS naics_support_services 
       ,CASE WHEN sp.naics_education_services = 1 THEN ii.naics_education_services ELSE rw.naics_education_services END                AS naics_education_services 
       ,CASE WHEN sp.naics_health = 1 THEN ii.naics_health ELSE rw.naics_health END                                                    AS naics_health 
       ,CASE WHEN sp.naics_infor_culture = 1 THEN ii.naics_infor_culture ELSE rw.naics_infor_culture END                               AS naics_infor_culture 
       ,CASE WHEN sp.naics_accomdation_food = 1 THEN ii.naics_accomdation_food ELSE rw.naics_accomdation_food END                      AS naics_accomdation_food 
       ,CASE WHEN sp.naics_other = 1 THEN ii.naics_other ELSE rw.naics_other END                                                       AS naics_other 
       ,CASE WHEN sp.naics_public_admin = 1 THEN ii.naics_public_admin ELSE rw.naics_public_admin END                                  AS naics_public_admin 
       ,CASE WHEN sp.nocs_employed = 1 THEN ii.nocs_employed ELSE rw.nocs_employed END                                                 AS nocs_employed 
       ,CASE WHEN sp.nocs_management = 1 THEN ii.nocs_management ELSE rw.nocs_management END                                           AS nocs_management 
       ,CASE WHEN sp.nocs_management_senior = 1 THEN ii.nocs_management_senior ELSE rw.nocs_management_senior END                      AS nocs_management_senior 
       ,CASE WHEN sp.nocs_management_specialized = 1 THEN ii.nocs_management_specialized ELSE rw.nocs_management_specialized END       AS nocs_management_specialized 
       ,CASE WHEN sp.nocs_management_retail = 1 THEN ii.nocs_management_retail ELSE rw.nocs_management_retail END                      AS nocs_management_retail 
       ,CASE WHEN sp.nocs_management_trades = 1 THEN ii.nocs_management_trades ELSE rw.nocs_management_trades END                      AS nocs_management_trades 
       ,CASE WHEN sp.nocs_business = 1 THEN ii.nocs_business ELSE rw.nocs_business END                                                 AS nocs_business 
       ,CASE WHEN sp.nocs_business_pro = 1 THEN ii.nocs_business_pro ELSE rw.nocs_business_pro END                                     AS nocs_business_pro 
       ,CASE WHEN sp.nocs_business_admin = 1 THEN ii.nocs_business_admin ELSE rw.nocs_business_admin END                               AS nocs_business_admin 
       ,CASE WHEN sp.nocs_business_finance = 1 THEN ii.nocs_business_finance ELSE rw.nocs_business_finance END                         AS nocs_business_finance 
       ,CASE WHEN sp.nocs_business_office = 1 THEN ii.nocs_business_office ELSE rw.nocs_business_office END                            AS nocs_business_office 
       ,CASE WHEN sp.nocs_business_distribution = 1 THEN ii.nocs_business_distribution ELSE rw.nocs_business_distribution END          AS nocs_business_distribution 
       ,CASE WHEN sp.nocs_sciences = 1 THEN ii.nocs_sciences ELSE rw.nocs_sciences END                                                 AS nocs_sciences 
       ,CASE WHEN sp.nocs_sciences_pro = 1 THEN ii.nocs_sciences_pro ELSE rw.nocs_sciences_pro END                                     AS nocs_sciences_pro 
       ,CASE WHEN sp.nocs_sciences_tech = 1 THEN ii.nocs_sciences_tech ELSE rw.nocs_sciences_tech END                                  AS nocs_sciences_tech 
       ,CASE WHEN sp.nocs_health = 1 THEN ii.nocs_health ELSE rw.nocs_health END                                                       AS nocs_health 
       ,CASE WHEN sp.nocs_health_nursing = 1 THEN ii.nocs_health_nursing ELSE rw.nocs_health_nursing END                               AS nocs_health_nursing 
       ,CASE WHEN sp.nocs_health_pro = 1 THEN ii.nocs_health_pro ELSE rw.nocs_health_pro END                                           AS nocs_health_pro 
       ,CASE WHEN sp.nocs_health_tech = 1 THEN ii.nocs_health_tech ELSE rw.nocs_health_tech END                                        AS nocs_health_tech 
       ,CASE WHEN sp.nocs_health_assist = 1 THEN ii.nocs_health_assist ELSE rw.nocs_health_assist END                                  AS nocs_health_assist 
       ,CASE WHEN sp.nocs_public = 1 THEN ii.nocs_public ELSE rw.nocs_public END                                                       AS nocs_public 
       ,CASE WHEN sp.nocs_public_education = 1 THEN ii.nocs_public_education ELSE rw.nocs_public_education END                         AS nocs_public_education 
       ,CASE WHEN sp.nocs_public_pro = 1 THEN ii.nocs_public_pro ELSE rw.nocs_public_pro END                                           AS nocs_public_pro 
       ,CASE WHEN sp.nocs_public_para = 1 THEN ii.nocs_public_para ELSE rw.nocs_public_para END                                        AS nocs_public_para 
       ,CASE WHEN sp.nocs_public_protection = 1 THEN ii.nocs_public_protection ELSE rw.nocs_public_protection END                      AS nocs_public_protection 
       ,CASE WHEN sp.nocs_public_support = 1 THEN ii.nocs_public_support ELSE rw.nocs_public_support END                               AS nocs_public_support 
       ,CASE WHEN sp.nocs_culture = 1 THEN ii.nocs_culture ELSE rw.nocs_culture END                                                    AS nocs_culture 
       ,CASE WHEN sp.nocs_culture_pro = 1 THEN ii.nocs_culture_pro ELSE rw.nocs_culture_pro END                                        AS nocs_culture_pro 
       ,CASE WHEN sp.nocs_culture_tech = 1 THEN ii.nocs_culture_tech ELSE rw.nocs_culture_tech END                                     AS nocs_culture_tech 
       ,CASE WHEN sp.nocs_sales = 1 THEN ii.nocs_sales ELSE rw.nocs_sales END                                                          AS nocs_sales 
       ,CASE WHEN sp.nocs_sales_retail = 1 THEN ii.nocs_sales_retail ELSE rw.nocs_sales_retail END                                     AS nocs_sales_retail 
       ,CASE WHEN sp.nocs_sales_service = 1 THEN ii.nocs_sales_service ELSE rw.nocs_sales_service END                                  AS nocs_sales_service 
       ,CASE WHEN sp.nocs_sales_wholesale = 1 THEN ii.nocs_sales_wholesale ELSE rw.nocs_sales_wholesale END                            AS nocs_sales_wholesale 
       ,CASE WHEN sp.nocs_sales_other = 1 THEN ii.nocs_sales_other ELSE rw.nocs_sales_other END                                        AS nocs_sales_other 
       ,CASE WHEN sp.nocs_sales_support = 1 THEN ii.nocs_sales_support ELSE rw.nocs_sales_support END                                  AS nocs_sales_support 
       ,CASE WHEN sp.nocs_sales_nec = 1 THEN ii.nocs_sales_nec ELSE rw.nocs_sales_nec END                                              AS nocs_sales_nec 
       ,CASE WHEN sp.nocs_trades = 1 THEN ii.nocs_trades ELSE rw.nocs_trades END                                                       AS nocs_trades 
       ,CASE WHEN sp.nocs_trades_industrial = 1 THEN ii.nocs_trades_industrial ELSE rw.nocs_trades_industrial END                      AS nocs_trades_industrial 
       ,CASE WHEN sp.nocs_trades_maintenance = 1 THEN ii.nocs_trades_maintenance ELSE rw.nocs_trades_maintenance END                   AS nocs_trades_maintenance 
       ,CASE WHEN sp.nocs_trades_other = 1 THEN ii.nocs_trades_other ELSE rw.nocs_trades_other END                                     AS nocs_trades_other 
       ,CASE WHEN sp.nocs_trades_heavy = 1 THEN ii.nocs_trades_heavy ELSE rw.nocs_trades_heavy END                                     AS nocs_trades_heavy 
       ,CASE WHEN sp.nocs_trades_helpers = 1 THEN ii.nocs_trades_helpers ELSE rw.nocs_trades_helpers END                               AS nocs_trades_helpers 
       ,CASE WHEN sp.nocs_agriculture = 1 THEN ii.nocs_agriculture ELSE rw.nocs_agriculture END                                        AS nocs_agriculture 
       ,CASE WHEN sp.nocs_agriculture_supervisor = 1 THEN ii.nocs_agriculture_supervisor ELSE rw.nocs_agriculture_supervisor END       AS nocs_agriculture_supervisor 
       ,CASE WHEN sp.nocs_agriculture_workers = 1 THEN ii.nocs_agriculture_workers ELSE rw.nocs_agriculture_workers END                AS nocs_agriculture_workers 
       ,CASE WHEN sp.nocs_agriculture_harvesting = 1 THEN ii.nocs_agriculture_harvesting ELSE rw.nocs_agriculture_harvesting END       AS nocs_agriculture_harvesting 
       ,CASE WHEN sp.nocs_manufacturing = 1 THEN ii.nocs_manufacturing ELSE rw.nocs_manufacturing END                                  AS nocs_manufacturing 
       ,CASE WHEN sp.nocs_manufacturing_supervisor = 1 THEN ii.nocs_manufacturing_supervisor ELSE rw.nocs_manufacturing_supervisor END AS nocs_manufacturing_supervisor 
       ,CASE WHEN sp.nocs_manufacturing_operators = 1 THEN ii.nocs_manufacturing_operators ELSE rw.nocs_manufacturing_operators END    AS nocs_manufacturing_operators 
       ,CASE WHEN sp.nocs_manufacturing_assemblers = 1 THEN ii.nocs_manufacturing_assemblers ELSE rw.nocs_manufacturing_assemblers END AS nocs_manufacturing_assemblers 
       ,CASE WHEN sp.nocs_manufacturing_labourers = 1 THEN ii.nocs_manufacturing_labourers ELSE rw.nocs_manufacturing_labourers END    AS nocs_manufacturing_labourers
    FROM {{ref('all_wide_raw')}} AS rw
    INNER JOIN {{ref('all_suppression')}} AS sp USING(id)
    INNER JOIN {{ref('all_imputation_input')}} AS ii USING(id)
),
calc_pct AS (
    SELECT  id 
        ,code
        ,ref_year 
        ,geo_name 
        ,geo_parent_name 
        ,pop15p 
        ,CASE WHEN (lbf + not_in_lbf) = 0 THEN 0 ELSE lbf / (lbf + not_in_lbf) END                                                            AS lbf 
        ,CASE WHEN (lbf + not_in_lbf) = 0 THEN 0 ELSE not_in_lbf / (lbf + not_in_lbf) END                                                     AS not_in_lbf 
        ,CASE WHEN (employed + unemployed) = 0 THEN 0 ELSE employed / (employed + unemployed) END                                             AS employed 
        ,CASE WHEN (employed + unemployed) = 0 THEN 0 ELSE unemployed / (employed + unemployed) END                                           AS unemployed 
        ,CASE WHEN (employed_full + employed_part) = 0 THEN 0 ELSE employed_full / (employed_full + employed_part) END                        AS employed_full 
        ,CASE WHEN (employed_full + employed_part) = 0 THEN 0 ELSE employed_part / (employed_full + employed_part) END                        AS employed_part 
        ,CASE WHEN (naics_goods + naics_services) = 0 THEN 0 ELSE naics_goods / (naics_goods + naics_services) END                            AS naics_goods 
        ,CASE WHEN (naics_goods + naics_services) = 0 THEN 0 ELSE naics_services / (naics_goods + naics_services) END                         AS naics_services 
        ,CASE WHEN (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) = 0 THEN 0 ELSE naics_agriculture / (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) END       AS naics_agriculture 
        ,CASE WHEN (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) = 0 THEN 0 ELSE naics_natural_resources / (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) END AS naics_natural_resources 
        ,CASE WHEN (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) = 0 THEN 0 ELSE naics_ultilities / (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) END        AS naics_ultilities 
        ,CASE WHEN (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) = 0 THEN 0 ELSE naics_construction / (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) END      AS naics_construction 
        ,CASE WHEN (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) = 0 THEN 0 ELSE naics_manufacturing / (naics_agriculture + naics_natural_resources + naics_ultilities + naics_construction + naics_manufacturing) END     AS naics_manufacturing 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_retail / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END                AS naics_retail 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_transport_warehousing / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END AS naics_transport_warehousing 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_financial / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END             AS naics_financial 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_professional / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END          AS naics_professional 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_support_services / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END      AS naics_support_services 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_education_services / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END    AS naics_education_services 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_health / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END                AS naics_health 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_infor_culture / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END         AS naics_infor_culture 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_accomdation_food / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END      AS naics_accomdation_food 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_other / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END                 AS naics_other 
        ,CASE WHEN (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) = 0 THEN 0 ELSE naics_public_admin / (naics_retail + naics_transport_warehousing + naics_financial + naics_professional + naics_support_services + naics_education_services + naics_health + naics_infor_culture + naics_accomdation_food + naics_other + naics_public_admin) END          AS naics_public_admin 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_management / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END    AS nocs_management 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_business / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END      AS nocs_business 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_sciences / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END      AS nocs_sciences 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_health / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END        AS nocs_health 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_public / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END        AS nocs_public 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_culture / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END       AS nocs_culture 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_sales / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END         AS nocs_sales 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_trades / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END        AS nocs_trades 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_agriculture / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END   AS nocs_agriculture 
        ,CASE WHEN (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) = 0 THEN 0 ELSE nocs_manufacturing / (nocs_management + nocs_business + nocs_sciences + nocs_health + nocs_public + nocs_culture + nocs_sales + nocs_trades + nocs_agriculture + nocs_manufacturing) END AS nocs_manufacturing 
        ,CASE WHEN (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) = 0 THEN 0 ELSE nocs_management_senior / (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) END      AS nocs_management_senior 
        ,CASE WHEN (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) = 0 THEN 0 ELSE nocs_management_specialized / (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) END AS nocs_management_specialized 
        ,CASE WHEN (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) = 0 THEN 0 ELSE nocs_management_retail / (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) END      AS nocs_management_retail 
        ,CASE WHEN (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) = 0 THEN 0 ELSE nocs_management_trades / (nocs_management_senior + nocs_management_specialized + nocs_management_retail + nocs_management_trades) END      AS nocs_management_trades 
        ,CASE WHEN (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) = 0 THEN 0 ELSE nocs_business_pro / (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) END          AS nocs_business_pro 
        ,CASE WHEN (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) = 0 THEN 0 ELSE nocs_business_admin / (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) END        AS nocs_business_admin 
        ,CASE WHEN (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) = 0 THEN 0 ELSE nocs_business_finance / (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) END      AS nocs_business_finance 
        ,CASE WHEN (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) = 0 THEN 0 ELSE nocs_business_office / (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) END       AS nocs_business_office 
        ,CASE WHEN (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) = 0 THEN 0 ELSE nocs_business_distribution / (nocs_business_pro + nocs_business_admin + nocs_business_finance + nocs_business_office + nocs_business_distribution) END AS nocs_business_distribution 
        ,CASE WHEN (nocs_sciences_pro + nocs_sciences_tech) = 0 THEN 0 ELSE nocs_sciences_pro / (nocs_sciences_pro + nocs_sciences_tech) END  AS nocs_sciences_pro 
        ,CASE WHEN (nocs_sciences_pro + nocs_sciences_tech) = 0 THEN 0 ELSE nocs_sciences_tech / (nocs_sciences_pro + nocs_sciences_tech) END AS nocs_sciences_tech 
        ,CASE WHEN (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) = 0 THEN 0 ELSE nocs_health_nursing / (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) END AS nocs_health_nursing 
        ,CASE WHEN (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) = 0 THEN 0 ELSE nocs_health_pro / (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) END     AS nocs_health_pro 
        ,CASE WHEN (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) = 0 THEN 0 ELSE nocs_health_tech / (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) END    AS nocs_health_tech 
        ,CASE WHEN (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) = 0 THEN 0 ELSE nocs_health_assist / (nocs_health_nursing + nocs_health_pro + nocs_health_tech + nocs_health_assist) END  AS nocs_health_assist 
        ,CASE WHEN (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) = 0 THEN 0 ELSE nocs_public_education / (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) END  AS nocs_public_education 
        ,CASE WHEN (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) = 0 THEN 0 ELSE nocs_public_pro / (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) END        AS nocs_public_pro 
        ,CASE WHEN (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) = 0 THEN 0 ELSE nocs_public_para / (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) END       AS nocs_public_para 
        ,CASE WHEN (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) = 0 THEN 0 ELSE nocs_public_protection / (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) END AS nocs_public_protection 
        ,CASE WHEN (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) = 0 THEN 0 ELSE nocs_public_support / (nocs_public_education + nocs_public_pro + nocs_public_para + nocs_public_protection + nocs_public_support) END    AS nocs_public_support 
        ,CASE WHEN (nocs_culture_pro + nocs_culture_tech) = 0 THEN 0 ELSE nocs_culture_pro / (nocs_culture_pro + nocs_culture_tech) END       AS nocs_culture_pro 
        ,CASE WHEN (nocs_culture_pro + nocs_culture_tech) = 0 THEN 0 ELSE nocs_culture_tech / (nocs_culture_pro + nocs_culture_tech) END      AS nocs_culture_tech 
        ,CASE WHEN (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) = 0 THEN 0 ELSE nocs_sales_retail / (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) END    AS nocs_sales_retail 
        ,CASE WHEN (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) = 0 THEN 0 ELSE nocs_sales_service / (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) END   AS nocs_sales_service 
        ,CASE WHEN (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) = 0 THEN 0 ELSE nocs_sales_wholesale / (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) END AS nocs_sales_wholesale 
        ,CASE WHEN (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) = 0 THEN 0 ELSE nocs_sales_other / (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) END     AS nocs_sales_other 
        ,CASE WHEN (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) = 0 THEN 0 ELSE nocs_sales_support / (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) END   AS nocs_sales_support 
        ,CASE WHEN (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) = 0 THEN 0 ELSE nocs_sales_nec / (nocs_sales_retail + nocs_sales_service + nocs_sales_wholesale + nocs_sales_other + nocs_sales_support + nocs_sales_nec) END       AS nocs_sales_nec 
        ,CASE WHEN (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) = 0 THEN 0 ELSE nocs_trades_industrial / (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) END  AS nocs_trades_industrial 
        ,CASE WHEN (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) = 0 THEN 0 ELSE nocs_trades_maintenance / (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) END AS nocs_trades_maintenance 
        ,CASE WHEN (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) = 0 THEN 0 ELSE nocs_trades_other / (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) END       AS nocs_trades_other 
        ,CASE WHEN (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) = 0 THEN 0 ELSE nocs_trades_heavy / (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) END       AS nocs_trades_heavy 
        ,CASE WHEN (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) = 0 THEN 0 ELSE nocs_trades_helpers / (nocs_trades_industrial + nocs_trades_maintenance + nocs_trades_other + nocs_trades_heavy + nocs_trades_helpers) END     AS nocs_trades_helpers 
        ,CASE WHEN (nocs_agriculture_supervisor + nocs_agriculture_workers + nocs_agriculture_harvesting) = 0 THEN 0 ELSE nocs_agriculture_supervisor / (nocs_agriculture_supervisor + nocs_agriculture_workers + nocs_agriculture_harvesting) END AS nocs_agriculture_supervisor 
        ,CASE WHEN (nocs_agriculture_supervisor + nocs_agriculture_workers + nocs_agriculture_harvesting) = 0 THEN 0 ELSE nocs_agriculture_workers / (nocs_agriculture_supervisor + nocs_agriculture_workers + nocs_agriculture_harvesting) END    AS nocs_agriculture_workers 
        ,CASE WHEN (nocs_agriculture_supervisor + nocs_agriculture_workers + nocs_agriculture_harvesting) = 0 THEN 0 ELSE nocs_agriculture_harvesting / (nocs_agriculture_supervisor + nocs_agriculture_workers + nocs_agriculture_harvesting) END AS nocs_agriculture_harvesting 
        ,CASE WHEN (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) = 0 THEN 0 ELSE nocs_manufacturing_supervisor / (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) END AS nocs_manufacturing_supervisor 
        ,CASE WHEN (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) = 0 THEN 0 ELSE nocs_manufacturing_operators / (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) END  AS nocs_manufacturing_operators 
        ,CASE WHEN (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) = 0 THEN 0 ELSE nocs_manufacturing_assemblers / (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) END AS nocs_manufacturing_assemblers 
        ,CASE WHEN (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) = 0 THEN 0 ELSE nocs_manufacturing_labourers / (nocs_manufacturing_supervisor + nocs_manufacturing_operators + nocs_manufacturing_assemblers + nocs_manufacturing_labourers) END  AS nocs_manufacturing_labourers
    FROM apply_imputes
),
level_1 AS (
    SELECT  id 
        ,pop15p 
        ,pop15p * lbf        AS lbf
        ,pop15p * not_in_lbf AS not_in_lbf
    FROM calc_pct
),
level_2 AS (
    SELECT  a.id 
        ,a.pop15p 
        ,a.lbf 
        ,a.not_in_lbf 
        ,a.lbf * b.employed   AS employed
        ,a.lbf * b.unemployed AS unemployed
    FROM level_1 a
    INNER JOIN  calc_pct b USING (id)
),
level_3 AS (
    SELECT  a.id
        ,a.pop15p 
        ,a.lbf 
        ,a.employed 
        ,a.employed * b.employed_full      AS employed_full 
        ,a.employed * b.employed_part      AS employed_part
        ,a.unemployed 
        ,a.not_in_lbf 
        ,a.employed * b.naics_goods        AS naics_goods 
        ,a.employed * b.naics_services     AS naics_services 
        ,a.employed * b.nocs_management    AS nocs_management 
        ,a.employed * b.nocs_business      AS nocs_business 
        ,a.employed * b.nocs_sciences      AS nocs_sciences 
        ,a.employed * b.nocs_health        AS nocs_health 
        ,a.employed * b.nocs_public        AS nocs_public 
        ,a.employed * b.nocs_culture       AS nocs_culture 
        ,a.employed * b.nocs_sales         AS nocs_sales 
        ,a.employed * b.nocs_trades        AS nocs_trades 
        ,a.employed * b.nocs_agriculture   AS nocs_agriculture 
        ,a.employed * b.nocs_manufacturing AS nocs_manufacturing
    FROM level_2 a
    INNER JOIN calc_pct b USING(id)
),
all_wide_clean AS (
    SELECT  a.id 
        ,b.code 
        ,b.ref_year
        ,b.geo_name 
        ,b.geo_parent_name 
        ,a.pop15p 
        ,a.lbf 
        ,a.employed 
        ,a.employed_full 
        ,a.employed_part 
        ,a.unemployed
        ,a.not_in_lbf 
        ,a.naics_goods 
        ,a.naics_goods * b.naics_agriculture       AS naics_agriculture
        ,a.naics_goods * b.naics_natural_resources AS naics_natural_resources
        ,a.naics_goods * b.naics_ultilities        AS naics_ultilities
        ,a.naics_goods * b.naics_construction      AS naics_construction
        ,a.naics_goods * b.naics_manufacturing     AS naics_manufacturing
        ,a.naics_services 
        ,a.naics_services * b.naics_retail                AS naics_retail  
        ,a.naics_services * b.naics_transport_warehousing AS naics_transport_warehousing
        ,a.naics_services * b.naics_financial             AS naics_financial  
        ,a.naics_services * b.naics_professional          AS naics_professional  
        ,a.naics_services * b.naics_support_services      AS naics_support_services  
        ,a.naics_services * b.naics_education_services    AS naics_education_services  
        ,a.naics_services * b.naics_health                AS naics_health  
        ,a.naics_services * b.naics_infor_culture         AS naics_infor_culture  
        ,a.naics_services * b.naics_accomdation_food      AS naics_accomdation_food  
        ,a.naics_services * b.naics_other                 AS naics_other  
        ,a.naics_services * b.naics_public_admin          AS naics_public_admin  
        ,a.nocs_management 
        ,a.nocs_management * b.nocs_management_senior      AS nocs_management_senior  
        ,a.nocs_management * b.nocs_management_specialized AS nocs_management_specialized
        ,a.nocs_management * b.nocs_management_retail      AS nocs_management_retail  
        ,a.nocs_management * b.nocs_management_trades      AS nocs_management_trades  
        ,a.nocs_business 
        ,a.nocs_business * b.nocs_business_pro          AS nocs_business_pro 
        ,a.nocs_business * b.nocs_business_admin        AS nocs_business_admin 
        ,a.nocs_business * b.nocs_business_finance      AS nocs_business_finance 
        ,a.nocs_business * b.nocs_business_office       AS nocs_business_office 
        ,a.nocs_business * b.nocs_business_distribution AS nocs_business_distribution
        ,a.nocs_sciences 
        ,a.nocs_sciences * b.nocs_sciences_pro  AS nocs_sciences_pro 
        ,a.nocs_sciences * b.nocs_sciences_tech AS nocs_sciences_tech
        ,a.nocs_health 
        ,a.nocs_health * b.nocs_health_nursing  AS nocs_health_nursing 
        ,a.nocs_health * b.nocs_health_pro      AS nocs_health_pro 
        ,a.nocs_health * b.nocs_health_tech     AS nocs_health_tech 
        ,a.nocs_health * b.nocs_health_assist   AS nocs_health_assist 
        ,a.nocs_public 
        ,a.nocs_public * b.nocs_public_education  AS nocs_public_education 
        ,a.nocs_public * b.nocs_public_pro        AS nocs_public_pro 
        ,a.nocs_public * b.nocs_public_para       AS nocs_public_para 
        ,a.nocs_public * b.nocs_public_protection AS nocs_public_protection 
        ,a.nocs_public * b.nocs_public_support    AS nocs_public_support 
        ,a.nocs_culture 
        ,a.nocs_culture * b.nocs_culture_pro  AS nocs_culture_pro 
        ,a.nocs_culture * b.nocs_culture_tech AS nocs_culture_tech 
        ,a.nocs_sales 
        ,a.nocs_sales * b.nocs_sales_retail    AS nocs_sales_retail 
        ,a.nocs_sales * b.nocs_sales_service   AS nocs_sales_service 
        ,a.nocs_sales * b.nocs_sales_wholesale AS nocs_sales_wholesale 
        ,a.nocs_sales * b.nocs_sales_other     AS nocs_sales_other 
        ,a.nocs_sales * b.nocs_sales_support   AS nocs_sales_support 
        ,a.nocs_sales * b.nocs_sales_nec       AS nocs_sales_nec 
        ,a.nocs_trades 
        ,a.nocs_trades * b.nocs_trades_industrial  AS nocs_trades_industrial 
        ,a.nocs_trades * b.nocs_trades_maintenance AS nocs_trades_maintenance 
        ,a.nocs_trades * b.nocs_trades_other       AS nocs_trades_other 
        ,a.nocs_trades * b.nocs_trades_heavy       AS nocs_trades_heavy 
        ,a.nocs_trades * b.nocs_trades_helpers     AS nocs_trades_helpers 
        ,a.nocs_agriculture 
        ,a.nocs_agriculture * b.nocs_agriculture_supervisor AS nocs_agriculture_supervisor 
        ,a.nocs_agriculture * b.nocs_agriculture_workers    AS nocs_agriculture_workers 
        ,a.nocs_agriculture * b.nocs_agriculture_harvesting AS nocs_agriculture_harvesting 
        ,a.nocs_manufacturing
        ,a.nocs_manufacturing * b.nocs_manufacturing_supervisor AS nocs_manufacturing_supervisor 
        ,a.nocs_manufacturing * b.nocs_manufacturing_operators  AS nocs_manufacturing_operators 
        ,a.nocs_manufacturing * b.nocs_manufacturing_assemblers AS nocs_manufacturing_assemblers 
        ,a.nocs_manufacturing * b.nocs_manufacturing_labourers  AS nocs_manufacturing_labourers
    FROM level_3 a
    INNER JOIN calc_pct b USING(id)
)
SELECT * FROM all_wide_clean
