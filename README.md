# Ember data in datasette

## Install

pip install datasette sqlite-utils

## Create database

sqlite-utils insert ember.db annual data/annual/2021_annual_database.csv --csv  
sqlite-utils insert ember.db monthly data/monthly/202108_monthly_database.csv --csv

## Run (on local machine)

datasette ember.db
