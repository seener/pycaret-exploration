WITH year_info AS (
    SELECT DISTINCT "REF_DATE" as ref_year
    FROM labourforce_14100090
    ORDER BY ref_year
)
SELECT * FROM year_info