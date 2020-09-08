WITH temp AS (
	SELECT ref_year, "GEO", attribute, CASE WHEN status_flag = '' THEN 0 ELSE 1 END AS suppres_flag
	FROM {{ref('labourforce')}}
	UNION
	SELECT ref_year, "GEO", attribute, CASE WHEN status_flag = '' THEN 0 ELSE 1 END AS suppres_flag
	FROM {{ref('naics')}}
	UNION
	SELECT ref_year, "GEO", attribute, CASE WHEN status_flag = '' THEN 0 ELSE 1 END AS suppres_flag
	FROM {{ref('nocs')}}
),
temp_sum AS (
	SELECT  ref_year, "GEO", SUM(suppres_flag) AS total_suppress, COUNT(*) AS total_col
	FROM temp
	GROUP BY ref_year, "GEO"
),
suppress AS (
	SELECT geo.id, ts.total_suppress, ts.total_col
	FROM temp_sum AS ts
	INNER JOIN dev.geo_year AS geo ON ts.ref_year = geo.ref_year AND ts."GEO" = geo."GEO"
),
all_suppression AS (
    SELECT lbf.*,
        {{dbt_utils.star(from=ref('naics_suppression'), except=['id', 'ref_year', 'code', 'geo_name', 'geo_level_id', 'geo_level_name', 'geo_parent_name'])}},
        {{dbt_utils.star(from=ref('nocs_suppression'),  except=['id', 'ref_year', 'code', 'geo_name', 'geo_level_id', 'geo_level_name', 'geo_parent_name'])}},
        suppress.total_col,
        suppress.total_suppress
    FROM {{ref('labourforce_suppression')}} AS lbf
    INNER JOIN {{ref('naics_suppression')}} AS nac USING("id")
    INNER JOIN {{ref('nocs_suppression')}}  AS noc USING("id")
    INNER JOIN suppress USING("id")
)
SELECT * FROM all_suppression