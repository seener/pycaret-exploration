WITH geo_info AS (
     SELECT DISTINCT
          CASE WHEN LEFT("DGUID", 4) = '' THEN NULL
               WHEN "GEO" = 'Canada' THEN '01'
               WHEN CAST(LEFT("DGUID", 4) AS INTEGER) = 2011 THEN RIGHT("DGUID", 4)
               ELSE RIGHT("DGUID", 2) 
          END AS code,
          CASE WHEN "GEO" = 'Canada' THEN 1
               WHEN split_part("GEO", ',', 2) = '' THEN 2
               ELSE 3 
          END AS geo_level_id,
          CASE WHEN "GEO" = 'Canada' THEN 'Country'
               WHEN split_part("GEO", ',', 2) = '' THEN 'Province'
               ELSE 'Economic Region' 
          END AS geo_level_name,
          trim(split_part("GEO", ',', 1)) AS geo_name,
          "GEO",
          CASE WHEN "GEO" = 'Canada' THEN NULL
               WHEN split_part("GEO", ',', 2) = '' THEN 'Canada'
               ELSE trim(split_part("GEO", ',', 2)) 
          END AS geo_parent_name,
          CASE WHEN "GEO" = 'Canada' THEN NULL
               WHEN split_part("GEO", ',', 2) = '' THEN '01'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Newfoundland and Labrador' THEN '10'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Prince Edward Island' THEN '11'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Nova Scotia' THEN '12'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'New Brunswick' THEN '13'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Quebec' THEN '24'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Ontario' THEN '35'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Manitoba' THEN '46'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Saskatchewan' THEN '47'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Alberta' THEN '48'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'British Columbia' THEN '59'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Yukon' THEN '60'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Northwest Territories' THEN '61'
               WHEN "DGUID" = '' AND trim(split_part("GEO", ',', 2)) = 'Nunavut' THEN '62'
               ELSE LEFT(RIGHT("DGUID", 4), 2) 
          END AS parent_code,
          CASE WHEN LEFT("DGUID", 4) = '' THEN NULL
               ELSE CAST(LEFT("DGUID", 4) AS INTEGER) 
          END AS geo_vintage
     FROM labourforce_14100090
     ORDER BY geo_level_id, parent_code, code
)
SELECT * FROM geo_info