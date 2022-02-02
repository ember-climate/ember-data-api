INSERT IGNORE INTO published.api_generation_yearly (country_or_region, country_code, "year", variable,
    generation_twh, share_of_generation_pct, capacity_gw, emissions_mtco2, continent, eu_member_flag, 
    g20_flag, oecd_flag, region_demand_rank, latest_year, coal_deadline, clean_deadline)
WITH latest_year as(
    SELECT country_code, MAX("year") as max_year 
    FROM published.mart_generation_yearly_global 
    WHERE "year" <= {api_year}
    GROUP BY country_code
), region_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(PARTITION BY continent ORDER BY demand_twh DESC) as region_demand_rank
    FROM mart_country_overview_yearly_global
    WHERE "year" = {api_year} - 1
), deadlines as(
    SELECT
        country_name,
        coal_deadline,
        clean_deadline
    FROM dim_country
)
SELECT
    generation.country_name as country_or_region,
    generation.country_code,
    "year",
    global_fuel_desc as variable,
    generation_twh,
    share_of_generation_pct,
    capacity_gw,
    emissions_estimate_mtco2 as emissions_mtco2,
    continent,
    eu_member_flag,
    g20_flag,
    oecd_flag,
    region_demand_rank.region_demand_rank as region_demand_rank,
    latest_year.max_year as latest_year,
    deadlines.coal_deadline as coal_deadline,
    deadlines.clean_deadline as clean_deadline
FROM published.mart_generation_yearly_global generation
LEFT JOIN latest_year
    ON generation.country_code = latest_year.country_code
LEFT JOIN region_demand_rank
    ON generation.country_name = region_demand_rank.country_name
LEFT JOIN deadlines
    ON generation.country_name = deadlines.country_name
WHERE "year" <= {api_year}
