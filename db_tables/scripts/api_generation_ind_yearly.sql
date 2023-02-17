INSERT INTO published.api_generation_ind_yearly (
    country,
    "state",
    "year",
    variable,
    generation_gwh,
    share_of_generation_pct,
    capacity_mw,
    emissions_ktco2,
    share_of_emissions_pct
)
WITH combined as (
    SELECT 
        country_name
        , state_name
        , "year"
        , global_fuel_desc
        , generation_gwh
        , share_of_generation_pct
        , capacity_mw
        , emissions_ktco2
        , share_of_emissions_pct
    FROM published.mart_generation_yearly_ind_states
    UNION ALL
    SELECT 
        country_name
        , state_name
        , "year"
        , 'Wind and solar' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(capacity_mw)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_yearly_ind_states
    WHERE global_fuel_desc in ('Wind', 'Solar')
    GROUP BY country_name, state_name, "year"
    UNION ALL
    SELECT 
        country_name
        , state_name
        , "year"
        , 'Fossil' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(capacity_mw)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_yearly_ind_states
    WHERE global_fuel_desc in ('Other Fossil', 'Coal', 'Gas')
    GROUP BY country_name, state_name, "year"
    UNION ALL
    SELECT 
        country_name
        , state_name
        , "year"
        , 'Clean' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(capacity_mw)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_yearly_ind_states
    WHERE global_fuel_desc in ('Other Renewables', 'Nuclear', 'Hydro', 'Wind', 'Solar', 'Bioenergy')
    GROUP BY country_name, state_name, "year"
    UNION ALL
    SELECT 
        country_name
        , state_name
        , "year"
        , 'Renewables' as global_fuel_desc
        , SUM(generation_gwh)
        , SUM(share_of_generation_pct)
        , SUM(capacity_mw)
        , SUM(emissions_ktco2)
        , SUM(share_of_emissions_pct)
    FROM published.mart_generation_yearly_ind_states
    WHERE global_fuel_desc in ('Other Renewables', 'Hydro', 'Wind', 'Solar', 'Bioenergy')
    GROUP BY country_name, state_name, "year"
)
SELECT 
    'India' as country,
    states.state_name as "state",
    states.year as "year",
    states.global_fuel_desc as variable,
    states.generation_gwh as generation_gwh,
    states.share_of_generation_pct as share_of_generation_pct,
    states.capacity_mw as capacity_mw,
    states.emissions_ktco2 as emissions_ktco2,
    states.share_of_emissions_pct as share_of_emissions_pct
FROM combined as states