WITH suppression AS (
    SELECT 
        ref_year,
        "GEO",
        attribute, 
        CASE WHEN status_flag = '' THEN 0 ELSE 1 END AS suppress_flag
    FROM {{ref('labourforce')}}
),
labour_suppression AS (
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
    FROM {{ref('labourforce')}} AS lf
    INNER JOIN {{ref('attribute_short_name')}} AS names ON lf.attribute = names.description
    INNER JOIN {{ref('geo_year')}} AS geo ON geo."GEO" = lf."GEO" AND geo.ref_year = lf.ref_year
    INNER JOIN suppression ON lf.ref_year = suppression.ref_year AND lf."GEO" = suppression."GEO" AND lf.attribute = suppression.attribute
    GROUP BY geo.id, lf.ref_year, geo.code, geo.geo_name, geo.geo_level_name, geo.geo_parent_name
    ORDER BY geo.id
)
SELECT id, ref_year, code, geo_name, geo_parent_name, geo_level_name,
pop15p, lbf, employed, employed_full, employed_part, unemployed, not_in_lbf
FROM labour_suppression