import pandas as pd

from sqlalchemy.engine.base import Connection

from utils.handle_db_connections import handle_db_connections


@handle_db_connections
def _read_monthly_prices_countries(published_con: Connection) -> pd.DataFrame:
    return pd.read_sql("""
                SELECT 
                    dap.price_date AS "Date", 
                    cou.country_name,
                    dap.price_eur_per_mwh
                FROM fact_day_ahead_price_monthly dap
                LEFT JOIN dim_country cou
                        ON dap.country_code = cou.country_code
                WHERE price_date >= '2018-01-01'
                """, published_con)


@handle_db_connections
def _read_monthly_prices_maxmin(published_con: Connection) -> pd.DataFrame:
    return pd.read_sql("""
                SELECT 
                    price_date AS "Date", 
                    MAX(price_eur_per_mwh) AS "Max EU price", 
                    MIN(price_eur_per_mwh) AS "Min EU price"
                FROM fact_day_ahead_price_monthly
                WHERE price_date >= '2018-01-01'
                GROUP BY 1
                """, published_con)


def _transform_monthly_prices_country(df: pd.DataFrame) -> pd.DataFrame:
    return pd.pivot_table(df,
                          values='price_eur_per_mwh',
                          index='Date',
                          columns='country_name').reset_index()


def _combine_prices(country_df: pd.DataFrame, maxmin_df: pd.DataFrame) -> pd.DataFrame:
    return maxmin_df.merge(country_df, how='left', on='Date')


def create_api_day_ahead_price_monthly() -> pd.DataFrame:
    prices_countries = _read_monthly_prices_countries()
    transformed_prices_countries = _transform_monthly_prices_country(prices_countries)
    prices_maxmin = _read_monthly_prices_maxmin()
    return _combine_prices(transformed_prices_countries, prices_maxmin)


def main() -> pd.DataFrame:
    return create_api_day_ahead_price_monthly()


if __name__ == '__main__':
    main()
