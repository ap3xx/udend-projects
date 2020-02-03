class EtlQueries:
    ft_crimes_insert = ("""
        SELECT
            c.id,
            c.case_number,
            cl.id,
            c.iucr,
            cast(c.date as timestamp),
            ld.id,
            c.arrest,
            c.domestic,
            c.latitude,
            c.longitude
        FROM staging_crimes c
        LEFT JOIN dim_location_description ld
        ON c.location_description = ld.description
        LEFT JOIN dim_city_location cl
        ON (
            COALESCE(c.block, '') = COALESCE(cl.block, '') AND
            COALESCE(c.beat, 0) = COALESCE(cl.beat, 0) AND
            COALESCE(c.district, 0) = COALESCE(cl.district, 0) AND
            COALESCE(c.ward, 0) = COALESCE(cl.ward, 0) AND
            COALESCE(c.community_area, 0) = COALESCE(cl.community_area, 0)
        )
    """)

    dim_crime_description_insert = ("""
        SELECT DISTINCT iucr, primary_description, secondary_description, index_code
        FROM staging_crime_descriptions
    """)

    dim_location_description_insert = ("""
        (description)
        SELECT DISTINCT location_description
        FROM staging_crimes
    """)

    dim_city_location_insert = ("""
        (block, beat, district, ward, community_area) 
        SELECT DISTINCT block, beat, district, ward, community_area
        FROM staging_crimes
    """)

    dim_time_insert = ("""
        SELECT 
            crime_date, 
            extract(hour from crime_date) as hour, 
            extract(day from crime_date) as day,
            extract(dayofweek from crime_date) as dayofweek,
            extract(week from crime_date) as week, 
            extract(month from crime_date) as month, 
            extract(month from crime_date) as quarter, 
            extract(year from crime_date) as year
        FROM (
            SELECT DISTINCT CAST(date as TIMESTAMP) as crime_date
            FROM staging_crimes
        )
    """)
