# Ember data in datasette

## Install

pip install datasette sqlite-utils

## Create database

run update_api.py

## Create legacy database

sqlite-utils insert ember.db generation_annual data/generation_annual_database.csv --csv --detect-types
sqlite-utils insert ember.db capacity_annual data/capacity_annual_database.csv --csv --detect-types
sqlite-utils insert ember.db intensity_annual data/intensity_annual_database.csv --csv --detect-types

## Test (on local machine)

datasette ember.db
