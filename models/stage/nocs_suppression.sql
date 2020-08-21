WITH suppression AS (
    SELECT 
        ref_year,
        "GEO",
        attribute, 
        CASE WHEN status_flag = '' THEN 0 ELSE 1 END AS suppress_flag
    FROM {{ref('nocs')}}
),
nocs_suppression AS (
        SELECT
        geo.id,
        lf.ref_year,
        geo.code, 
        geo.geo_name, 
        geo.geo_parent_name,
        geo.geo_level_name, 
        {{dbt_utils.pivot(
            column='short_name',
            values=dbt_utils.get_column_values(
                table=ref('attribute_short_name'), 
                column='short_name'),
            then_value='suppress_flag',
            else_value=0
        )}}
    FROM {{ref('nocs')}} AS lf
    INNER JOIN {{ref('attribute_short_name')}} AS names ON lf.attribute = names.description
    INNER JOIN {{ref('geo_year')}} AS geo ON geo."GEO" = lf."GEO" AND geo.ref_year = lf.ref_year
    INNER JOIN suppression ON lf.ref_year = suppression.ref_year AND lf."GEO" = suppression."GEO" AND lf.attribute = suppression.attribute
    GROUP BY geo.id, lf.ref_year, geo.code, geo.geo_name, geo.geo_level_name, geo.geo_parent_name
    ORDER BY geo.id
)
SELECT id, ref_year, code, geo_name, geo_parent_name, geo_level_name,
nocs_employed, 
nocs_management, nocs_management_senior, nocs_management_specialized, nocs_management_retail, nocs_management_trades,
nocs_business, nocs_business_pro, nocs_business_admin, nocs_business_finance, nocs_business_office, nocs_business_distribution, 
nocs_sciences, nocs_sciences_pro, nocs_sciences_tech, 
nocs_health, nocs_health_nursing, nocs_health_pro, nocs_health_tech, nocs_health_assist, 
nocs_public, nocs_public_education, nocs_public_pro, nocs_public_para, nocs_public_protection, nocs_public_support, 
nocs_culture, nocs_culture_pro, nocs_culture_tech, 
nocs_sales, nocs_sales_retail, nocs_sales_service, nocs_sales_wholesale, nocs_sales_other, nocs_sales_support, nocs_sales_nec, nocs_trades, 
nocs_trades_industrial, nocs_trades_maintenance, nocs_trades_other, nocs_trades_heavy, nocs_trades_helpers, 
nocs_agriculture, nocs_agriculture_supervisor, nocs_agriculture_workers, nocs_agriculture_harvesting, 
nocs_manufacturing, nocs_manufacturing_supervisor, nocs_manufacturing_operators, nocs_manufacturing_assemblers, nocs_manufacturing_labourers
FROM nocs_suppression