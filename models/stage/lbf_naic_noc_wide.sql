WITH lbf_naic_noc_wide AS (
    SELECT 
        lbf.*,
        {{dbt_utils.star(from=ref('naics_wide'), except=['ref_year', 'code', 'geo_name', 'geo_level_id', 'geo_level_name', 'geo_parent_name'])}},
        {{dbt_utils.star(from=ref('nocs_wide'),  except=['ref_year', 'code', 'geo_name', 'geo_level_id', 'geo_level_name', 'geo_parent_name'])}}
    FROM {{ref('labourforce_wide')}} AS lbf
    INNER JOIN {{ref('naics_wide')}} AS nac ON lbf.ref_year = nac.ref_year AND lbf.geo_name = nac.geo_name
    INNER JOIN {{ref('nocs_wide')}}  AS noc ON lbf.ref_year = noc.ref_year AND lbf.geo_name = noc.geo_name
)
SELECT * FROM lbf_naic_noc_wide