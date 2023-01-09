INSERT INTO published.api_generation_usa_yearly (
    country,
    "state",
    "year",
    variable,
    generation_gwh,
    share_of_generation_pct,
    emissions_ktco2,
    share_of_emissions_pct
)
SELECT 
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE country.country_name END as country,
    states.state_name as "state",
    states.year as "year",
    states.global_fuel_desc as variable,
    states.generation_gwh as generation_gwh,
    states.share_of_generation_pct as share_of_generation_pct,
    states.emissions_ktco2 as emissions_ktco2,
    states.share_of_emissions_pct as share_of_emissions_pct
FROM published.mart_generation_yearly_usa_states states
LEFT JOIN published.dim_country country
    ON country.country_code = states.country_code