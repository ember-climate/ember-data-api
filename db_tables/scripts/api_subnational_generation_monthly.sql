INSERT INTO published.api_subnational_generation_monthly (
    country,
    "state",
    "date",
    variable,
    generation_gwh,
    capacity_gw
)
SELECT 
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE country.country_name END as country,
    generation.state_name as "state",
    generation.generation_date as "date",
    generation.global_fuel_code as variable,
    generation.generation_gwh as generation_gwh,
    capacity.capacity_gw as capacity_gw
FROM transformed.trn_gen_mth_ind_states_hist generation
LEFT JOIN published.dim_country country
    ON country.country_code = generation.country_code
LEFT JOIN transformed.trn_cap_mth_ind_states_hist capacity
    ON 
        capacity.state_code = generation.state_code
        AND capacity.capacity_date = generation.generation_date
        AND capacity.global_fuel_code = generation.global_fuel_code


