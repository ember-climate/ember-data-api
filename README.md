# Ember data in datasette

## Install

pip install datasette sqlite-utils

## Create database

sqlite-utils insert ember.db annual data/annual_generation_api.csv --csv --detect-types

## Run (on local machine)

datasette ember.db
