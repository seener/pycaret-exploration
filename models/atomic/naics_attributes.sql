WITH naics_attributes as (
    SELECT distinct 
        CAST(split_part("COORDINATE", '.', 2) AS INTEGER) AS order_id,
        trim("UOM") as measurement,
        trim("North American Industry Classification System (NAICS)") as attribute,
        trim("SCALAR_FACTOR") as scalar,
        CASE WHEN trim("North American Industry Classification System (NAICS)") = 'Total employed, all industries' THEN NULL
            WHEN trim("North American Industry Classification System (NAICS)") = 'Goods-producing sector' THEN 'Total employed, all industries'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Agriculture [111-112, 1100, 1151-1152]' THEN 'Goods-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]' THEN 'Goods-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Utilities [22]' THEN 'Goods-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Construction [23]' THEN 'Goods-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Manufacturing [31-33]' THEN 'Goods-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Services-producing sector' THEN 'Total employed, all industries'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Accommodation and food services [72]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Business, building and other support services [55-56]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Educational services [61]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Finance, insurance, real estate, rental and leasing [52-53]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Health care and social assistance [62]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Information, culture and recreation [51, 71]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Other services (except public administration) [81]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Professional, scientific and technical services [54]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Public administration [91]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Transportation and warehousing [48-49]' THEN 'Services-producing sector'
            WHEN trim("North American Industry Classification System (NAICS)") = 'Wholesale and retail trade [41, 44-45]' THEN 'Services-producing sector'
        END AS attribute_parent,
        CASE WHEN trim("North American Industry Classification System (NAICS)") = 'Total employed, all industries' THEN 1
            WHEN trim("North American Industry Classification System (NAICS)") = 'Goods-producing sector' THEN 2
            WHEN trim("North American Industry Classification System (NAICS)") = 'Services-producing sector' THEN 2
            ELSE 3 
        END AS attribute_level_id
    FROM emp_naics_14100092
    ORDER BY order_id, attribute
)
SELECT * FROM naics_attributes