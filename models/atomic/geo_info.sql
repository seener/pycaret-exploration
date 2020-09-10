WITH geo_info AS (
     SELECT DISTINCT
          CASE WHEN "GEO" = 'Canada' THEN '01'
               WHEN "GEO" = 'South Coast-Burin Peninsula and Notre Dame-Central Bonavista Bay, Newfoundland and Labrador' THEN '1099'
               WHEN "GEO" = 'Côte-Nord and Nord-du-Québec, Quebec' THEN '2499'
               WHEN "GEO" = 'Parklands and Northern, Manitoba' THEN '4699'
               WHEN "GEO" = 'South Central and North Central, Manitoba' THEN '4689'
               WHEN "GEO" = 'Prince Albert and Northern, Saskatchewan' THEN '4799'
               WHEN "GEO" = 'Banff-Jasper-Rocky Mountain House and Athabasca-Grande Prairie-Peace River, Alberta' THEN '4899'
               WHEN "GEO" = 'North Coast and Nechako, British Columbia' THEN '5999'
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