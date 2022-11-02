INSERT INTO published.api_subnational_generation_monthly (
    country,
    "state",
    "date",
    variable,
    generation_gwh,
    capacity_gw,
    emissions_mtco2
)
SELECT 
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE country.country_name END as country,
    states.state_name as "state",
    states.generation_date as "date",
    fuel.global_fuel_desc as variable,
    states.generation_gwh as generation_gwh,
    states.capacity_gw as capacity_gw,
    states.emissions_mtco2 as emissions_mtco2
FROM transformed.mart_generation_monthly_states states
LEFT JOIN published.dim_country country
    ON country.country_code = generation.country_code
LEFT JOIN published.dim_global_fuel_code fuel
    ON fuel.global_fuel_code = generation.global_fuel_code


