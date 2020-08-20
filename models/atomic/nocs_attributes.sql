WITH nocs_attributes AS (
    SELECT DISTINCT
        CAST(split_part("COORDINATE", '.', 2) AS INTEGER) AS order_id,
        trim("UOM") as measurement,
        trim("National Occupational Classification (NOC)") as attribute,
        trim("SCALAR_FACTOR") as scalar,
        CASE WHEN trim("National Occupational Classification (NOC)") = 'Total employed, all occupations' THEN NULL
            ELSE 'Total employed, all occupations' 
        END as attribute_parent,
        CASE WHEN trim("National Occupational Classification (NOC)") = 'Total employed, all occupations' THEN 1
            ELSE 2 
        END as attribute_level_id
    FROM emp_nocs_14100312
    ORDER BY order_id
)
SELECT * FROM nocs_attributes