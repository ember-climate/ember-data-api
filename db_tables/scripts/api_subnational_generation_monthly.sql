INSERT INTO published.api_subnational_generation_monthly (
    country,
    "state",
    "date",
    variable,
    generation_gwh,
    capacity_mw,
    emissions_ktco2
)
SELECT 
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE country.country_name END as country,
    states.state_name as "state",
    states.generation_date as "date",
    states.global_fuel_desc as variable,
    states.generation_gwh as generation_gwh,
    states.capacity_mw as capacity_mw,
    states.emissions_ktco2 as emissions_ktco2
FROM staging.mart_generation_monthly_states states
LEFT JOIN published.dim_country country
    ON country.country_code = states.country_code


