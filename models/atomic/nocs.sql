WITH nocs AS (
    SELECT 
        "REF_DATE" AS ref_year, 
        "GEO", 
        trim("National Occupational Classification (NOC)") AS attribute, 
        "VALUE" as measure, 
        trim("STATUS") as status_flag 
    FROM emp_nocs_14100312
)
SELECT * FROM nocs