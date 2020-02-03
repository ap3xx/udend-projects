class ReCreateDWQueries: 
    """
    Contains the SQL queries to drop and recreate tables at Redshift. 
    """
    drop_dim_time = ("""
        DROP TABLE IF EXISTS public.dim_time
    """)
    
    create_dim_time = ("""
        CREATE TABLE public.dim_time (
            datetime timestamp NOT NULL PRIMARY KEY,
            "hour" int4,
            "day" int4,
            weekday int4,
            week int4,
            "month" int4,
            quarter int4,
            "year" int4
        )
    """)
    
    drop_dim_crime_description = ("""
        DROP TABLE IF EXISTS public.dim_crime_description
    """)
        
    create_dim_crime_description = ("""
        CREATE TABLE public.dim_crime_description (
            iucr varchar(20) NOT NULL PRIMARY KEY,
            primary_description varchar(256),
            secondary_description varchar(256),
            index_code varchar(20)
        )
        DISTSTYLE ALL
    """)
    
    drop_dim_city_location = ("""
        DROP TABLE IF EXISTS public.dim_city_location
    """)
        
    create_dim_city_location = ("""
        CREATE TABLE public.dim_city_location (
            id bigint identity(1, 1) PRIMARY KEY,
            block varchar(256),
            beat int4,
            district int4,
            ward int4,
            community_area int4
        )
    """)
    
    drop_dim_location_description = ("""
        DROP TABLE IF EXISTS public.dim_location_description
    """)
        
    create_dim_location_description = ("""
        CREATE TABLE public.dim_location_description (
            id bigint identity(1, 1) PRIMARY KEY,
            description varchar(256)
        )
        DISTSTYLE ALL
    """)
    
    drop_ft_crimes = ("""
        DROP TABLE IF EXISTS public.ft_crimes
    """)
    
    create_ft_crimes = ("""
        CREATE TABLE public.ft_crimes (
            id bigint NOT NULL PRIMARY KEY,
            case_number varchar(100) NOT NULL,
            city_location_id bigint NOT NULL,
            iucr varchar(20) NOT NULL,
            crime_date timestamp NOT NULL,
            location_description_id bigint NOT NULL,
            arrest boolean,
            domestic boolean,
            latitude float8,
            longitude float8,
            FOREIGN KEY(city_location_id) REFERENCES dim_city_location(id),
            FOREIGN KEY(iucr) REFERENCES dim_crime_description(iucr),
            FOREIGN KEY(crime_date) REFERENCES dim_time(datetime),
            FOREIGN KEY(location_description_id) REFERENCES dim_location_description(id)
        )
    """)
    
    drop_staging_crimes = ("""
        DROP TABLE IF EXISTS public.staging_crimes
    """)
    
    create_staging_crimes = ("""
        CREATE TABLE public.staging_crimes (
            id bigint NOT NULL,
            case_number varchar(100) NOT NULL,
            date varchar(50) NOT NULL,
            block varchar(256) NOT NULL,
            iucr varchar(20) NOT NULL,
            location_description varchar(256) NOT NULL,
            arrest boolean,
            domestic boolean,
            beat int4,
            district int4,
            ward int4,
            community_area int4,
            latitude float8,
            longitude float8
        )
    """)

    drop_staging_crime_descriptions = ("""
        DROP TABLE IF EXISTS public.staging_crime_descriptions;
    """)
    
    create_staging_crime_descriptions = ("""
        CREATE TABLE public.staging_crime_descriptions (
            iucr varchar(20) NOT NULL,
            primary_description varchar(256),
            secondary_description varchar(256),
            index_code varchar(20)
        )
    """)
