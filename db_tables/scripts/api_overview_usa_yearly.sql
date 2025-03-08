INSERT INTO published.api_overview_usa_yearly (
    country,
    "state",
    "year",
    demand_gwh,
    emissions_ktco2,
    emissions_intensity_gco2_per_kwh
)
SELECT 
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE country.country_name END as country,
    states.state_name as "state",
    states.year as "year",
    states.demand_gwh as demand_gwh,
    states.emissions_ktco2 as emissions_ktco2,
    states.emissions_intensity_gco2_per_kwh as emissions_intensity_gco2_per_kwh
FROM published.mart_overview_yearly_usa_states states
LEFT JOIN published.dim_country country
    ON country.country_code = states.country_code
WHERE states.year <= 2023
