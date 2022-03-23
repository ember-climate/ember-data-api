INSERT INTO published.api_country_overview_yearly (country_or_region, country_code, "year", 
    demand_twh, demand_mwh_per_capita, emissions_intensity_gco2_per_kwh, continent, ember_region, eu_flag, g20_flag, 
    g7_flag, oecd_flag, region_demand_rank, oecd_demand_rank, eu_demand_rank, latest_year, coal_deadline, clean_deadline)
WITH region_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(PARTITION BY ember_region ORDER BY demand_twh DESC) as region_demand_rank
    FROM mart_country_overview_yearly_global
    WHERE "year" = {api_year} - 1
), oecd_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(ORDER BY demand_twh  DESC) as oecd_demand_rank
    FROM mart_country_overview_yearly_global
    WHERE oecd_flag = 1 and year = {api_year} - 1
), eu_demand_rank as(
    SELECT
        country_name,
        row_number() OVER(ORDER BY demand_twh  DESC) as eu_demand_rank
    FROM mart_country_overview_yearly_global
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
        "year",
        demand_twh,
        demand_mwh_per_capita,
        emissions_intensity_estimate_gco2_per_kwh as emissions_intensity_gco2_per_kwh
    FROM published.mart_country_overview_yearly_global
    UNION
    SELECT 
        region as country_or_region,
        NULL as country_code,
        "year",
        demand_twh,
        demand_mwh_per_capita,
        emissions_intensity_gco2_per_kwh as emissions_intensity_gco2_per_kwh
    FROM published.mart_overview_yearly_region
), latest_year as(
    SELECT country_or_region, MAX("year") as max_year 
    FROM combined_overview
    WHERE "year" <= {api_year}
    GROUP BY country_or_region
)
SELECT
    overview.country_or_region as country_or_region,
    overview.country_code,
    "year",
    demand_twh,
    demand_mwh_per_capita,
    emissions_intensity_gco2_per_kwh,
    country.continent,
    country.ember_region,
    country.eu_member_flag as eu_flag,
    country.g20_flag,
    country.g7_flag,
    country.oecd_flag,
    region_demand_rank.region_demand_rank as region_demand_rank,
    oecd_demand_rank.oecd_demand_rank as oecd_demand_rank,
    eu_demand_rank.eu_demand_rank as eu_demand_rank,
    latest_year.max_year as latest_year,
    deadlines.coal_deadline as coal_deadline,
    deadlines.clean_deadline as clean_deadline
FROM combined_overview overview
LEFT JOIN latest_year
    ON overview.country_or_region = latest_year.country_or_region
LEFT JOIN region_demand_rank
    ON overview.country_or_region = region_demand_rank.country_name
LEFT JOIN deadlines
    ON overview.country_or_region = deadlines.country_or_region
LEFT JOIN published.dim_country as country
    ON overview.country_or_region = country.country_name
LEFT JOIN oecd_demand_rank
    ON overview.country_or_region = oecd_demand_rank.country_name
LEFT JOIN eu_demand_rank
    ON overview.country_or_region = eu_demand_rank.country_name
WHERE "year" BETWEEN 2000 AND {api_year}
    AND overview.country_or_region IS NOT NULL 
	AND overview.country_or_region NOT IN ('Bermuda', 'Western Sahara', 'Gibraltar', 'Niue', 'Saint Helena, Ascension and Tristan da Cunha', 'Timor-Leste')
	AND (overview.country_or_region, "year") != ('Indonesia', 2021)  
AND overview.country_or_region IS NOT NULL
