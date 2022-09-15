import pandas as pd

from sqlalchemy.engine.base import Connection

from utils.handle_db_connections import handle_db_connections


MAX_DATES = {
    'monthly': "'2018-01-01'",
    'daily': 'DATE_SUB((SELECT MAX(price_date) FROM fact_day_ahead_price_daily), INTERVAL 3 MONTH)'
}


@handle_db_connections
def _read_prices_countries(published_con: Connection, grain: str) -> pd.DataFrame:
    return pd.read_sql(f"""
                SELECT 
                    dap.price_date AS "Date", 
                    cou.country_name,
                    dap.price_eur_per_mwh
                FROM fact_day_ahead_price_{grain} dap
                LEFT JOIN dim_country cou
                        ON dap.country_code = cou.country_code
                WHERE price_date >= {MAX_DATES[grain]}
                """, published_con)


@handle_db_connections
def _read_prices_maxmin(published_con: Connection, grain: str) -> pd.DataFrame:
    return pd.read_sql(f"""
                SELECT 
                    price_date AS "Date", 
                    MAX(price_eur_per_mwh) AS "Max EU price", 
                    MIN(price_eur_per_mwh) AS "Min EU price"
                FROM fact_day_ahead_price_{grain}
                WHERE price_date >= {MAX_DATES[grain]}
                GROUP BY 1
                """, published_con)


def _transform_prices_country(df: pd.DataFrame) -> pd.DataFrame:
    return pd.pivot_table(df,
                          values='price_eur_per_mwh',
                          index='Date',
                          columns='country_name').reset_index()


def _combine_prices(country_df: pd.DataFrame, maxmin_df: pd.DataFrame) -> pd.DataFrame:
    return maxmin_df.merge(country_df, how='left', on='Date')


def _get_prices(grain: str):
    prices_countries = _read_prices_countries(grain=grain)
    transformed_prices_countries = _transform_prices_country(prices_countries)
    prices_maxmin = _read_prices_maxmin(grain=grain)
    combined_prices = _combine_prices(transformed_prices_countries, prices_maxmin)
    combined_prices['Grain'] = grain.capitalize()
    return combined_prices


def create_api_day_ahead_price() -> pd.DataFrame:
    monthly_prices = _get_prices(grain='monthly')
    daily_prices = _get_prices(grain='daily')
    return pd.concat([monthly_prices, daily_prices])


def main() -> pd.DataFrame:
    return create_api_day_ahead_price()


if __name__ == '__main__':
    main()
