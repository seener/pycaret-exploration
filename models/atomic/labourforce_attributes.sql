WITH labourforce_attributes AS (
    SELECT DISTINCT
        CAST(split_part("COORDINATE", '.', 2) AS INTEGER) AS order_id,
        trim("UOM") AS measurement,
        trim("Labour force characteristics") AS attribute,
        trim("SCALAR_FACTOR") AS scalar,
        CASE WHEN trim("Labour force characteristics") = 'Employment rate' THEN 'Participation rate'
            WHEN trim("Labour force characteristics") = 'Unemployment rate' THEN 'Participation rate' 
            WHEN trim("Labour force characteristics") = 'Labour force' THEN 'Population'
            WHEN trim("Labour force characteristics") = 'Not in labour force' THEN 'Population'
            WHEN trim("Labour force characteristics") = 'Employment' THEN 'Labour force'
            WHEN trim("Labour force characteristics") = 'Unemployment' THEN 'Labour force'
            WHEN trim("Labour force characteristics") = 'Full-time employment' THEN 'Employment'
            WHEN trim("Labour force characteristics") = 'Part-time employment' THEN 'Employment'
        END AS attribute_parent,
        CASE WHEN trim("Labour force characteristics") = 'Participation rate' THEN 1
            WHEN trim("Labour force characteristics") = 'Employment rate' THEN 2
            WHEN trim("Labour force characteristics") = 'Unemployment rate' THEN 2
            WHEN trim("Labour force characteristics") = 'Population' THEN 1
            WHEN trim("Labour force characteristics") = 'Labour force' THEN 2
            WHEN trim("Labour force characteristics") = 'Employment' THEN 3
            WHEN trim("Labour force characteristics") = 'Full-time employment' THEN 4
            WHEN trim("Labour force characteristics") = 'Part-time employment' THEN 4
            WHEN trim("Labour force characteristics") = 'Unemployment' THEN 3
            WHEN trim("Labour force characteristics") = 'Not in labour force' THEN 2
        END AS attribute_level_id
    FROM labourforce_14100090
    ORDER BY order_id
)
SELECT * FROM labourforce_attributes