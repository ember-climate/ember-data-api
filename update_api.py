from asyncio import subprocess
import os
import pandas as pd
import subprocess
import os
from connect_to_db import *
from create_table import *


def update_api():

    # Set the max year for the api
    api_year = 2021

    # Delete old ember.db file
    try:
        os.remove("ember.db")
        print("Old sqlite db removed. Creating new file.")
    except:
        print("No sql lite db file present. Creating new file.")

    published_con = connect_to_db('ember-published')

    dataset_list = ['api_generation_monthly',
                    'api_generation_yearly', 'api_country_overview_yearly']

    for table_name in dataset_list:

        print(f"Updating table {table_name}")

        # Create or update the api data tables
        table_structure = open(
            f"db_tables/schemas/{table_name}.txt", 'r').read()
        create_table(published_con, table_name, table_structure)

        sql_file = open(f"db_tables/scripts/{table_name}.sql", 'r')
        print("Executing sql script:", f"{table_name}.sql")
        published_con.execute(sql_file.read().format(api_year=2021))
        sql_file.close()

        # Read table from db
        db_table_df = pd.read_sql_table(
            table_name, published_con)
        db_table_df.to_csv(f"./data/{table_name}.csv", index=False)

        api_table_name = table_name.split("_", 1)[1]

        subprocess.call(
            f"sqlite-utils insert ember.db {api_table_name} ./data/{table_name}.csv --csv --detect-types",
            shell=True)

        print(f"{table_name} added to sqlite db as {api_table_name}")


def main():
    update_api()


if __name__ == "__main__":
    main()
