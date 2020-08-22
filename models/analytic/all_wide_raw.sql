WITH all_wide_raw AS (
    SELECT 
        lbf.*,
        {{dbt_utils.star(from=ref('naics_wide'), except=['id', 'ref_year', 'code', 'geo_name', 'geo_level_id', 'geo_level_name', 'geo_parent_name'])}},
        {{dbt_utils.star(from=ref('nocs_wide'),  except=['id', 'ref_year', 'code', 'geo_name', 'geo_level_id', 'geo_level_name', 'geo_parent_name'])}}
    FROM {{ref('labourforce_wide')}} AS lbf
    INNER JOIN {{ref('naics_wide')}} AS nac USING("id")
    INNER JOIN {{ref('nocs_wide')}}  AS noc USING("id")
)
SELECT * FROM all_wide_raw