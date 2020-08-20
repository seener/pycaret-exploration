WITH naics AS (
    SELECT 
        "REF_DATE" AS ref_year, 
        "GEO", 
        trim("North American Industry Classification System (NAICS)") AS attribute, 
        "VALUE" as measure, 
        trim("STATUS") as status_flag 
    FROM emp_naics_14100092
)
SELECT * FROM naics