WITH labourforce_wide AS (
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
    FROM {{ref('labourforce')}} AS lf
    INNER JOIN {{ref('attribute_short_name')}} AS names ON lf.attribute = names.description
    INNER JOIN {{ref('geo_year')}} AS geo ON geo."GEO" = lf."GEO" AND geo.ref_year = lf.ref_year
    GROUP BY geo.id, lf.ref_year, geo.code, geo.geo_name, geo.geo_level_name, geo.geo_parent_name
    ORDER BY geo.id
)
SELECT id, ref_year, code, geo_name, geo_parent_name, geo_level_name,
pop15p, lbf, employed, employed_full, employed_part, unemployed, not_in_lbf
FROM labourforce_wide