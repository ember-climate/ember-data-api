INSERT INTO published.api_subnational_overview_monthly (
    country,
    "state",
    "date",
    demand_gwh,
    emissions_ktco2,
    emissions_intensity_gco2_per_kwh
)
SELECT 
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE country.country_name END as country,
    states.state_name as "state",
    states.generation_date as "date",
    states.demand_gwh as demand_gwh,
    states.emissions_ktco2 as emissions_ktco2,
    states.emissions_intensity_gco2_per_kwh as emissions_intensity_gco2_per_kwh
FROM testing.mart_overview_monthly_states states
LEFT JOIN published.dim_country country
    ON country.country_code = states.country_code

