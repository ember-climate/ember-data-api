INSERT INTO published.api_country_overview_monthly (country_or_region, country_code, "date", 
    demand_twh, emissions_intensity_gco2_per_kwh, continent, ember_region, eu_flag, g20_flag, 
    g7_flag, oecd_flag, region_demand_rank, world_demand_rank, oecd_demand_rank, eu_demand_rank, latest_year, coal_deadline, clean_deadline)
WITH region_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(PARTITION BY ember_region ORDER BY demand_twh DESC) as region_demand_rank
    FROM mart_overview_yearly_global
    WHERE "year" = {api_year} - 1
), world_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(ORDER BY demand_twh  DESC) as oecd_demand_rank
    FROM mart_overview_yearly_global
    WHERE year = {api_year} - 1
), oecd_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(ORDER BY demand_twh  DESC) as oecd_demand_rank
    FROM mart_overview_yearly_global
    WHERE oecd_flag = 1 and year = {api_year} - 1
), eu_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(ORDER BY demand_twh  DESC) as eu_demand_rank
    FROM mart_overview_yearly_global
    WHERE eu_member_flag = 1 and year = {api_year} - 1
), deadlines as(
    SELECT
        country_name as country_or_region,
        coal_deadline,
        clean_deadline
    FROM dim_country
    UNION
    SELECT
        region as country_or_region,
        coal_deadline,
        clean_deadline
    FROM dim_region
), combined_overview as (
    SELECT
        country_name as country_or_region,
        country_code,
        generation_date,
        demand_twh,
        emissions_intensity_gco2_per_kwh
    FROM published.mart_overview_monthly_global
    WHERE NOT projected_estimate_flag
    UNION
    SELECT 
        gen.region as country_or_region,
        NULL as country_code,
        generation_date,
        demand_twh,
        emissions_intensity_gco2_per_kwh
    FROM published.mart_overview_monthly_region gen
    LEFT JOIN published.dim_region reg 
    ON gen.region = reg.region
    WHERE
        reg.include_monthly
        AND generation_date BETWEEN reg.monthly_start_date
        AND ((CURRENT_DATE) - INTERVAL 1 + reg.monthly_lag MONTH)
), latest_year as(
    SELECT
        country_or_region,
        MAX(year(generation_date)) as max_year
    FROM
        combined_overview
    WHERE
        year(generation_date) <= {api_year}
    GROUP BY
        country_or_region
        )
SELECT
    CASE
        WHEN country.country_name IS NOT NULL THEN country.display_name
        ELSE overview.country_or_region END as country_or_region,
    overview.country_code,
    generation_date as "date",
    demand_twh,
    emissions_intensity_gco2_per_kwh,
    country.continent,
    country.ember_region,
    country.eu_member_flag as eu_flag,
    country.g20_flag,
    country.g7_flag,
    country.oecd_flag,
    region_demand_rank.region_demand_rank as region_demand_rank,
    world_demand_rank.oecd_demand_rank as world_demand_rank,
    oecd_demand_rank.oecd_demand_rank as oecd_demand_rank,
    eu_demand_rank.eu_demand_rank as eu_demand_rank,
    latest_year.max_year as latest_year,
    deadlines.coal_deadline as coal_deadline,
    deadlines.clean_deadline as clean_deadline
FROM combined_overview overview
LEFT JOIN latest_year ON overview.country_or_region = latest_year.country_or_region
LEFT JOIN region_demand_rank
    ON overview.country_or_region = region_demand_rank.country_name
LEFT JOIN deadlines
    ON overview.country_or_region = deadlines.country_or_region
LEFT JOIN published.dim_country as country
    ON overview.country_or_region = country.country_name
LEFT JOIN world_demand_rank
    ON overview.country_or_region = world_demand_rank.country_name
LEFT JOIN oecd_demand_rank
    ON overview.country_or_region = oecd_demand_rank.country_name
LEFT JOIN eu_demand_rank
    ON overview.country_or_region = eu_demand_rank.country_name
WHERE generation_date > '2000-01-01'
    AND generation_date < '2024-04-01'
    AND overview.country_or_region IS NOT NULL 
ORDER BY
    country_or_region,
    generation_date
