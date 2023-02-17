INSERT INTO published.api_generation_usa_monthly (
    country,
    "state",
    "date",
    variable,
    generation_gwh,
    share_of_generation_pct,
    emissions_ktco2,
    share_of_emissions_pct
)
WITH combined as (
    SELECT 
        country_name
        , state_name
        , generation_date
        , global_fuel_desc
        , generation_gwh
        , share_of_generation_pct
        , emissions_ktco2
        , share_of_emissions_pct
    FROM published.mart_generation_monthly_usa_states
    UNION ALL
    SELECT 
        country_name
        , state_name
        , generation_date
        , 'Wind and solar' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_monthly_usa_states
    WHERE global_fuel_desc in ('Wind', 'Solar')
    GROUP BY country_name, state_name, generation_date
    UNION ALL
    SELECT 
        country_name
        , state_name
        , generation_date
        , 'Fossil' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_monthly_usa_states
    WHERE global_fuel_desc in ('Other Fossil', 'Coal', 'Gas')
    GROUP BY country_name, state_name, generation_date
    UNION ALL
    SELECT 
        country_name
        , state_name
        , generation_date
        , 'Clean' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_monthly_usa_states
    WHERE global_fuel_desc in ('Other Renewables', 'Nuclear', 'Hydro', 'Wind', 'Solar', 'Bioenergy')
    GROUP BY country_name, state_name, generation_date
    UNION ALL
    SELECT 
        country_name
        , state_name
        , generation_date
        , 'Renewables' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_monthly_usa_states
    WHERE global_fuel_desc in ('Other Renewables', 'Hydro', 'Wind', 'Solar', 'Bioenergy')
    GROUP BY country_name, state_name, generation_date
)
SELECT 
    'United States' as country,
    states.state_name as "state",
    states.generation_date as "date",
    states.global_fuel_desc as variable,
    states.generation_gwh as generation_gwh,
    states.share_of_generation_pct as share_of_generation_pct,
    states.emissions_ktco2 as emissions_ktco2,
    states.share_of_emissions_pct as share_of_emissions_pct
FROM combined as states


