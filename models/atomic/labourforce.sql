WITH labourforce AS (
    SELECT 
        "REF_DATE" AS ref_year, 
        "GEO", 
        trim("Labour force characteristics") AS attribute, 
        "VALUE" as measure, 
        trim("STATUS") as status_flag 
    FROM labourforce_14100090
)
SELECT * FROM labourforce