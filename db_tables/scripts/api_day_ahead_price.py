import pandas as pd

from sqlalchemy.engine.base import Connection

from utils.handle_db_connections import handle_db_connections


MAX_DATES = {
    'monthly': "'2018-01-01'",
    'daily (1 year)': 'DATE_ADD('
                      'DATE_SUB((SELECT MAX(price_date) FROM published.fact_day_ahead_price_daily), INTERVAL 1 YEAR), '
                      'INTERVAL 1 DAY)',  # Gets the most recent year's worth of data
    'daily (3 months)': 'DATE_ADD('
                        'DATE_SUB((SELECT MAX(price_date) FROM published.fact_day_ahead_price_daily), INTERVAL 3 MONTH), '
                        'INTERVAL 1 DAY)',
}


@handle_db_connections
def _read_prices_countries(published_con: Connection, grain: str) -> pd.DataFrame:
    return pd.read_sql(f"""
                SELECT 
                    dap.price_date AS "Date", 
                    cou.country_name,
                    dap.price_eur_per_mwh
                FROM fact_day_ahead_price_{grain.split(' ')[0]} dap
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
                FROM fact_day_ahead_price_{grain.split(' ')[0]}
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


def _add_popup_date_column(source_df: pd.DataFrame, grain: str) -> pd.DataFrame:
    time_grain = grain.split(' ')[0]
    if time_grain == 'daily':
        source_df['popup_date'] = pd.to_datetime(source_df['Date']).dt.strftime('%d %b %y (%a)')
    elif time_grain == 'monthly':
        source_df['popup_date'] = pd.to_datetime(source_df['Date']).dt.strftime('%b %y')
    return source_df


def _get_prices(grain: str) -> pd.DataFrame:
    prices_countries = _read_prices_countries(grain=grain)
    transformed_prices_countries = _transform_prices_country(prices_countries)
    prices_maxmin = _read_prices_maxmin(grain=grain)
    combined_prices = _combine_prices(transformed_prices_countries, prices_maxmin)
    combined_prices['Grain'] = grain.capitalize()
    return _add_popup_date_column(combined_prices, grain)


def create_api_day_ahead_price() -> pd.DataFrame:
    monthly_prices = _get_prices(grain='monthly')
    daily_prices_3_months = _get_prices(grain='daily (3 months)')
    daily_prices_year = _get_prices(grain='daily (1 year)')
    return pd.concat([monthly_prices, daily_prices_3_months, daily_prices_year])


def main() -> pd.DataFrame:
    return create_api_day_ahead_price()


if __name__ == '__main__':
    main()
