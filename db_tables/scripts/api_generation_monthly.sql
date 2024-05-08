INSERT INTO
    published.api_generation_monthly (
        country_or_region,
        country_code,
        "date",
        variable,
        generation_twh,
        share_of_generation_pct,
        emissions_mtco2,
        continent,
        ember_region,
        eu_flag,
        g20_flag,
        g7_flag,
        oecd_flag,
        region_demand_rank,
        oecd_demand_rank,
        eu_demand_rank,
        global_fuel_rank,
        latest_year,
        coal_deadline,
        clean_deadline
    ) WITH region_demand_rank as(
        SELECT
            country_name,
            row_number() OVER(
                PARTITION BY ember_region
                ORDER BY
                    demand_twh DESC
            ) as region_demand_rank
        FROM
            mart_overview_yearly_global
        WHERE
            "year" = {api_year} - 1
    ),
    oecd_demand_rank as(
        SELECT
            country_name,
            row_number() OVER(
                ORDER BY
                    demand_twh DESC
            ) as oecd_demand_rank
        FROM
            mart_overview_yearly_global
        WHERE
            oecd_flag = 1
            and year = {api_year} - 1
    ),
    eu_demand_rank as(
        SELECT
            country_name,
            row_number() OVER(
                ORDER BY
                    demand_twh DESC
            ) as eu_demand_rank
        FROM
            mart_overview_yearly_global
        WHERE
            eu_member_flag = 1
            and year = {api_year} - 1
    ),
    global_fuel_rank as(
        SELECT
            country_name,
            global_fuel_desc,
            row_number() OVER(
                PARTITION BY global_fuel_desc
                ORDER BY
                    generation_twh DESC
            ) as global_fuel_rank
        FROM
            mart_generation_yearly_global
        WHERE
            "year" = {api_year} - 1
    ),
    deadlines as(
        SELECT
            country_name as country_or_region,
            coal_deadline,
            clean_deadline
        FROM
            dim_country
        UNION
        SELECT
            region as country_or_region,
            coal_deadline,
            clean_deadline
        FROM
            dim_region
    ),
    combined_gen as (
        SELECT
            country_name as country_or_region,
            country_code,
            generation_date,
            global_fuel_desc as variable,
            generation_twh,
            share_of_generation_pct,
            emissions_mtco2,
            fossil_flag,
            wind_solar_flag,
            projected_estimate_flag
        FROM
            published.mart_generation_monthly_global generation
        UNION
        SELECT
            gen.region as country_or_region,
            NULL as country_code,
            generation_date,
            global_fuel_desc as variable,
            generation_twh,
            share_of_generation_pct,
            emissions_mtco2,
            fossil_flag,
            wind_solar_flag,
            0 as projected_estimate_flag
        FROM
            published.mart_generation_monthly_region gen
            LEFT JOIN published.dim_region reg 
            ON gen.region = reg.region
        WHERE
            reg.include_monthly
            AND generation_date BETWEEN reg.monthly_start_date
            AND (
                (CURRENT_DATE) - INTERVAL 1 + reg.monthly_lag MONTH
            )
    ),
    latest_actual_month as (
        SELECT
            country_or_region,
            MAX(
                CASE
                    WHEN NOT projected_estimate_flag THEN generation_date
                    ELSE NULL
                END
            ) as max_monthly_actual_generation_date
        FROM
            combined_gen
        GROUP BY
            country_or_region
    ),
    -- latest_selected_month as(
    --     SELECT
    --         country_or_region,
    --         CASE
    --             WHEN year(max_monthly_actual_generation_date) <= {api_year} THEN str_to_date(concat('01-12-', '{api_year}'), '%%d-%%m-%%Y')
    --             ELSE max_monthly_actual_generation_date
    --         END as max_selected_generation_date
    --     FROM
    --         latest_actual_month
    -- ),
    expanded_table as(
        SELECT
            country_or_region,
            country_code,
            generation_date,
            variable,
            generation_twh,
            share_of_generation_pct,
            emissions_mtco2
        FROM
            combined_gen
        UNION
        SELECT
            country_or_region,
            country_code,
            generation_date,
            CASE
                WHEN fossil_flag = 1 THEN 'Fossil'
                ELSE 'Clean'
            END as variable,
            SUM(generation_twh) as generation_twh,
            SUM(share_of_generation_pct) as share_of_generation_pct,
            SUM(emissions_mtco2) as emissions_mtco2
        FROM
            combined_gen
        GROUP BY
            country_or_region,
            country_code,
            generation_date,
            fossil_flag
        UNION
        SELECT
            country_or_region,
            country_code,
            generation_date,
            'Wind and solar' as variable,
            SUM(generation_twh) as generation_twh,
            SUM(share_of_generation_pct) as share_of_generation_pct,
            SUM(emissions_mtco2) as emissions_mtco2
        FROM
            combined_gen
        WHERE wind_solar_flag
        GROUP BY
            country_or_region,
            country_code,
            generation_date
    ),
    latest_year as(
        SELECT
            country_or_region,
            MAX(year(generation_date)) as max_year
        FROM
            expanded_table
        WHERE
            year(generation_date) <= {api_year}
        GROUP BY
            country_or_region
    )
SELECT
    CASE
        WHEN country.display_name IS NOT NULL THEN country.display_name
        ELSE generation.country_or_region END as country_or_region,
    generation.country_code,
    generation.generation_date as "date",
    generation.variable as variable,
    generation.generation_twh as generation_twh,
    generation.share_of_generation_pct as share_of_generation_pct,
    generation.emissions_mtco2 as emissions_mtco2,
    country.continent as continent,
    country.ember_region as ember_region,
    country.eu_member_flag as eu_flag,
    country.g20_flag as g20_flag,
    country.g7_flag as g7_flag,
    country.oecd_flag as oecd_flag,
    region_demand_rank.region_demand_rank as region_demand_rank,
    oecd_demand_rank.oecd_demand_rank as oecd_demand_rank,
    eu_demand_rank.eu_demand_rank as eu_demand_rank,
    global_fuel_rank.global_fuel_rank as global_fuel_rank,
    latest_year.max_year as latest_year,
    deadlines.coal_deadline as coal_deadline,
    deadlines.clean_deadline as clean_deadline
FROM
    expanded_table generation
    LEFT JOIN latest_year ON generation.country_or_region = latest_year.country_or_region
    LEFT JOIN region_demand_rank ON generation.country_or_region = region_demand_rank.country_name
    LEFT JOIN global_fuel_rank ON generation.country_or_region = global_fuel_rank.country_name
    AND generation.variable = global_fuel_rank.global_fuel_desc
    LEFT JOIN deadlines ON generation.country_or_region = deadlines.country_or_region
    LEFT JOIN dim_country country ON generation.country_or_region = country.country_name
    LEFT JOIN oecd_demand_rank ON generation.country_or_region = oecd_demand_rank.country_name
    LEFT JOIN eu_demand_rank ON generation.country_or_region = eu_demand_rank.country_name
    LEFT JOIN latest_actual_month ON generation.country_or_region = latest_actual_month.country_or_region
WHERE
    generation_date BETWEEN '2000-01-01'
    AND latest_actual_month.max_monthly_actual_generation_date
    AND generation_date < '2024-04-01'
    AND generation.country_or_region IS NOT NULL
ORDER BY
    country_or_region,
    generation_date,
    variable