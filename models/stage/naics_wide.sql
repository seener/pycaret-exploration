WITH naics_wide AS (
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
            then_value='measure',
            else_value=0
        )}}
    FROM {{ref('naics')}} AS lf
    INNER JOIN {{ref('attribute_short_name')}} AS names ON lf.attribute = names.description
    INNER JOIN {{ref('geo_year')}} AS geo ON geo."GEO" = lf."GEO" AND geo.ref_year = lf.ref_year
    GROUP BY geo.id, lf.ref_year, geo.code, geo.geo_name, geo.geo_level_name, geo.geo_parent_name
    ORDER BY geo.id
)
SELECT id, ref_year, code, geo_name, geo_parent_name, geo_level_name,
naics_employed, 
naics_goods, 
naics_agriculture, naics_natural_resources,naics_ultilities, naics_construction, naics_manufacturing, 
naics_services, 
naics_retail, naics_transport_warehousing, naics_financial, naics_professional, 
naics_support_services, naics_education_services, naics_health, naics_infor_culture, 
naics_accomdation_food, naics_other, naics_public_admin
FROM naics_wide