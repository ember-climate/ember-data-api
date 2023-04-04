import pandas as pd
import subprocess
import os

from importlib import import_module
from sqlalchemy.engine.base import Connection

from utils.create_table import create_table
from utils.handle_db_connections import handle_db_connections


# Set the max year for the api. Needs to be changed once new year data will be read in
API_YEAR = 2022

# Lay out the tables we use and how we process them
SQL_DATASET_LIST = [
    'api_generation_ind_monthly',
    'api_generation_usa_monthly',
    'api_generation_ind_yearly',
    'api_generation_usa_yearly',
    'api_overview_ind_monthly',
    'api_overview_usa_monthly',
    'api_overview_ind_yearly',
    'api_overview_usa_yearly',
    'api_price_monthly',
    'api_generation_monthly',
    'api_generation_yearly',
    'api_country_overview_yearly',

]
PY_DATASET_LIST = ['api_day_ahead_price']
NO_DB_TABLE_LIST = ['euromod_2022', 'generation_tur_monthly']


@handle_db_connections
def _create_csv(published_con: Connection, table_name: str) -> None:

    print(f"Updating table {table_name}")

    # Create or update the api data tables
    table_structure = open(
        f"db_tables/schemas/{table_name}.txt", 'r').read()
    create_table(published_con, table_name, table_structure)

    if table_name in SQL_DATASET_LIST:
        with open(f"db_tables/scripts/{table_name}.sql", 'r') as file:
            print("Executing sql script:", f"{table_name}.sql")
            published_con.execute(file.read().format(api_year=API_YEAR))

    elif table_name in PY_DATASET_LIST:
        module_name = f'{table_name}.py'
        print(f'Executing python script: {module_name}')
        api_df = import_module(f'db_tables.scripts.{table_name}').main()
        api_df.to_sql(name=table_name, con=published_con,
                      if_exists='append', index=False)

    # Read table from db
    db_table_df = pd.read_sql_table(
        table_name, published_con)
    db_table_df.to_csv(f"./data/{table_name}.csv", index=False)


def update_api() -> None:

    db_table_list = SQL_DATASET_LIST + PY_DATASET_LIST
    all_table_list = db_table_list + NO_DB_TABLE_LIST

    # Execute scripts to create csvs
    for table_name in db_table_list:
        _create_csv(table_name=table_name)

    # Delete old ember.db file
    try:
        os.remove("ember.db")
        print("Old sqlite db removed. Creating new file.")
    except:
        print("No sql lite db file present. Creating new file.")

    # Add csvs into ember.db
    for table_name in all_table_list:
        api_table_name = table_name.split(
            "_", 1)[1] if table_name in db_table_list else table_name
        subprocess.call(
            f"sqlite-utils insert ember.db {api_table_name} ./data/{table_name}.csv --csv --detect-types",
            shell=True)
        print(f"{table_name} added to sqlite db as {api_table_name}")


def main():
    update_api()


if __name__ == "__main__":
    main()
