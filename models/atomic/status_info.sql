WITH status_list AS (
    SELECT DISTINCT "STATUS" FROM labourforce_14100090
    UNION
    SELECT DISTINCT "STATUS" FROM emp_nocs_14100312
    UNION
    SELECT DISTINCT "STATUS" FROM emp_naics_14100092
),
status_info AS (
    SELECT DISTINCT 
        trim("STATUS") as status_flag,
        CASE WHEN trim("STATUS") = '..' THEN 'not available for a specific reference period'	
            WHEN trim("STATUS") = '<LOD' THEN 'less than the limit of detection'
            WHEN trim("STATUS") = '0s' THEN 'value rounded to 0 (zero) where there is a meaningful distinction between true zero and the value that was rounded'
            WHEN trim("STATUS") = 'A' THEN 'data quality: excellent'
            WHEN trim("STATUS") = 'B' THEN 'data quality: very good'
            WHEN trim("STATUS") = 'C' THEN 'data quality: good'
            WHEN trim("STATUS") = 'D' THEN 'data quality: acceptable'
            WHEN trim("STATUS") = 'E' THEN 'use with caution'
            WHEN trim("STATUS") = 'F' THEN 'too unreliable to be published'
            WHEN trim("STATUS") = '...' THEN 'not applicable'
            WHEN trim("STATUS") = 'p' THEN 'preliminary'
            WHEN trim("STATUS") = 'r' THEN 'revised'	
            WHEN trim("STATUS") = 'x' THEN 'suppressed to meet the confidentiality requirements of the Statistics Act'
            WHEN trim("STATUS") = 't' THEN 'terminated'
        END AS status_description
    FROM status_list
)
SELECT * FROM status_info