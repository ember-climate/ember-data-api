INSERT IGNORE INTO published.api_country_overview_yearly (country_or_region, country_code, "year", 
    demand_twh, demand_mwh_per_capita, emissions_intensity_gco2_per_kwh, continent, ember_region, eu_member_flag, g20_flag, 
    oecd_flag, region_demand_rank, latest_year, coal_deadline, clean_deadline)
WITH latest_year as(
    SELECT country_code, MAX("year") as max_year 
    FROM published.mart_country_overview_yearly_global 
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
    overview.country_name as country_or_region,
    overview.country_code,
    "year",
    demand_twh,
    demand_mwh_per_capita,
    emissions_intensity_estimate_gco2_per_kwh as emissions_intensity_gco2_per_kwh,
    continent,
    ember_region,
    eu_member_flag,
    g20_flag,
    oecd_flag,
    region_demand_rank.region_demand_rank as region_demand_rank,
    latest_year.max_year as latest_year,
    deadlines.coal_deadline as coal_deadline,
    deadlines.clean_deadline as clean_deadline
FROM published.mart_country_overview_yearly_global overview
LEFT JOIN latest_year
    ON overview.country_code = latest_year.country_code
LEFT JOIN region_demand_rank
    ON overview.country_name = region_demand_rank.country_name
LEFT JOIN deadlines
    ON overview.country_name = deadlines.country_name
WHERE "year" BETWEEN 2000 AND {api_year}
