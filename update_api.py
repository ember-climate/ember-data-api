import pandas as pd
import subprocess
import os

from importlib import import_module
from asyncio import subprocess

from utils.connect_to_db import connect_to_db
from utils.create_table import create_table

# Set the max year for the api. Needs to be changed once new year data will be read in
API_YEAR = 2021


def update_api():

    # # Delete old ember.db file
    try:
        os.remove("ember.db")
        print("Old sqlite db removed. Creating new file.")
    except:
        print("No sql lite db file present. Creating new file.")

    published_con = connect_to_db('ember-published')

    sql_dataset_list = [
        'api_price_monthly',
        'api_generation_monthly',
        'api_generation_yearly',
        'api_country_overview_yearly'
    ]
    py_dataset_list = ['api_day_ahead_price_monthly']
    db_table_list = sql_dataset_list + py_dataset_list

    for table_name in db_table_list:

        print(f"Updating table {table_name}")

        # Create or update the api data tables
        table_structure = open(
            f"db_tables/schemas/{table_name}.txt", 'r').read()
        create_table(published_con, table_name, table_structure)

        if table_name in sql_dataset_list:
            with open(f"db_tables/scripts/{table_name}.sql", 'r') as file:
                print("Executing sql script:", f"{table_name}.sql")
                published_con.execute(file.read().format(api_year=API_YEAR))

        elif table_name in py_dataset_list:
            module_name = f'{table_name}.py'
            print(f'Executing python script: {module_name}')
            api_df = import_module(f'db_tables.scripts.{table_name}').main()
            api_df.to_sql(name=table_name, con=published_con,
                          if_exists='append', index=False)

        # Read table from db
        db_table_df = pd.read_sql_table(
            table_name, published_con)
        db_table_df.to_csv(f"./data/{table_name}.csv", index=False)

        api_table_name = table_name.split("_", 1)[1]

        subprocess.call(
            f"sqlite-utils insert ember.db {api_table_name} ./data/{table_name}.csv --csv --detect-types",
            shell=True)

        print(f"{table_name} added to sqlite db as {api_table_name}")

    # Tables without db table
    no_db_table_list = ['euromod_2022']

    for table_name in no_db_table_list:
        subprocess.call(
            f"sqlite-utils insert ember.db {table_name} ./data/{table_name}.csv --csv --detect-types",
            shell=True)

        print(f"{table_name} added to sqlite db as {table_name}")


def main():
    update_api()


if __name__ == "__main__":
    main()
