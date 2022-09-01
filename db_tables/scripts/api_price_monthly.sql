INSERT INTO published.api_price_monthly (country_or_region, country_code, "date",
    day_ahead_price_eur_per_mwh, continent, ember_region, eu_flag, g20_flag, g7_flag, oecd_flag)
SELECT
    cou.display_name as country_or_region
    , pr.country_code
    , pr.price_date as "date"
    , pr.price_eur_per_mwh as day_ahead_price_eur_per_mwh
    , cou.continent
    , cou.ember_region
    , cou.eu_member_flag as eu_flag
    , cou.g20_flag
    , cou.g7_flag
    , cou.oecd_flag
FROM fact_day_ahead_price_monthly pr
LEFT JOIN published.dim_country cou
    ON pr.country_code=cou.country_code;