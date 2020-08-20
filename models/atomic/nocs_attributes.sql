WITH nocs_attributes AS (
    SELECT DISTINCT
        CAST(split_part("COORDINATE", '.', 2) AS INTEGER) AS order_id,
        trim("UOM") as measurement,
        trim("National Occupational Classification (NOC)") as attribute,
        trim("SCALAR_FACTOR") as scalar,
        CASE WHEN trim("National Occupational Classification (NOC)") = 'Total employed, all occupations' THEN NULL
            WHEN trim("National Occupational Classification (NOC)") IN 
                ('Management occupations [0]',
                'Business, finance and administration occupations [1]',
                'Natural and applied sciences and related occupations [2]',
                'Health occupations [3]',
                'Occupations in education, law and social, community and government services [4]',
                'Occupations in art, culture, recreation and sport [5]',
                'Sales and service occupations [6]',
                'Trades, transport and equipment operators and related occupations [7]',
                'Natural resources, agriculture and related production occupations [8]',
                'Occupations in manufacturing and utilities [9]')
            THEN 'Total employed, all occupations'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Senior management occupations [00]',
                'Specialized middle management occupations [01-05]',
                'Middle management occupations in retail and wholesale trade and customer services [06]',
                'Middle management occupations in trades, transportation, production and utilities [07-09]')
            THEN 'Management occupations [0]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Professional occupations in business and finance [11]',
                'Administrative and financial supervisors and administrative occupations [12]',
                'Finance, insurance and related business administrative occupations [13]',
                'Office support occupations [14]',
                'Distribution, tracking and scheduling co-ordination occupations [15]')
            THEN 'Business, finance and administration occupations [1]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Professional occupations in natural and applied sciences [21]',
                'Technical occupations related to natural and applied sciences [22]')
            THEN 'Natural and applied sciences and related occupations [2]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Professional occupations in nursing [30]',
                'Professional occupations in health (except nursing) [31]',
                'Technical occupations in health [32]',
                'Assisting occupations in support of health services [34]')
            THEN 'Health occupations [3]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Professional occupations in education services [40]',
                'Professional occupations in law and social, community and government services [41]',
                'Paraprofessional occupations in legal, social, community and education services [42]',
                'Occupations in front-line public protection services [43]',
                'Care providers and educational, legal and public protection support occupations [44]')
            THEN 'Occupations in education, law and social, community and government services [4]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Professional occupations in art and culture [51]',
                'Technical occupations in art, culture, recreation and sport [52]')
            THEN 'Occupations in art, culture, recreation and sport [5]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Retail sales supervisors and specialized sales occupations [62]',
                'Service supervisors and specialized service occupations [63]',
                'Sales representatives and salespersons - wholesale and retail trade [64]',
                'Service representatives and other customer and personal services occupations [65]',
                'Sales support occupations [66]', 
                'Service support and other service occupations, n.e.c. [67]')
            THEN 'Sales and service occupations [6]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Industrial, electrical and construction trades [72]',
                'Maintenance and equipment operation trades [73]',
                'Other installers, repairers and servicers and material handlers [74]',
                'Transport and heavy equipment operation and related maintenance occupations [75]',
                'Trades helpers, construction labourers and related occupations [76]')
            THEN 'Trades, transport and equipment operators and related occupations [7]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Supervisors and technical occupations in natural resources, agriculture and related production [82]',
                'Workers in natural resources, agriculture and related production [84]',
                'Harvesting, landscaping and natural resources labourers [86]')
            THEN 'Natural resources, agriculture and related production occupations [8]'
            WHEN trim("National Occupational Classification (NOC)") IN
                ('Occupations in manufacturing and utilities [9]',
                'Processing, manufacturing and utilities supervisors and central control operators [92]',
                'Processing and manufacturing machine operators and related production workers [94]',
                'Assemblers in manufacturing [95]',
                'Labourers in processing, manufacturing and utilities [96]')
            THEN 'Occupations in manufacturing and utilities [9]'
        END as attribute_parent,
        CASE WHEN trim("National Occupational Classification (NOC)") = 'Total employed, all occupations' THEN 1
            WHEN  trim("National Occupational Classification (NOC)") IN 
                    ('Management occupations [0]',
                    'Business, finance and administration occupations [1]',
                    'Natural and applied sciences and related occupations [2]',
                    'Health occupations [3]',
                    'Occupations in education, law and social, community and government services [4]',
                    'Occupations in art, culture, recreation and sport [5]',
                    'Sales and service occupations [6]',
                    'Trades, transport and equipment operators and related occupations [7]',
                    'Natural resources, agriculture and related production occupations [8]',
                    'Occupations in manufacturing and utilities [9]')
            THEN 2
            ELSE 3 
        END as attribute_level_id
    FROM emp_nocs_14100312
    ORDER BY order_id
)
SELECT * FROM nocs_attributes