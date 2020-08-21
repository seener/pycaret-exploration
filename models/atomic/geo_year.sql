WITH geo_year AS(
    SELECT ROW_NUMBER() OVER(ORDER BY geo_level_id, parent_code, code) AS id, *
    FROM {{ref('geo_info')}}, {{ref('date_info')}}
)
SELECT * FROM geo_year