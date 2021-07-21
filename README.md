# Ember - Energy Data API

## Stack
Django + Django Rest Framework + drf-spectacular for OpenAPI 3.0 schema generation

## API

Global Energy Generation at `/global?area=<AREA>&fueltype=<FUEL_TYPE>&year=<YYYY>`  

## Import data

Quick and dirty, use `django-csvimport` and upload csv with matching model definitions through the admin panel.  
CSV needs to have a trailing comma on the headers or things break (presumably as the model includes some auto-filling time columns)  

## Improvements

Could use `django-countries` to define Country fields according to ISO3166-1

## Develop
Use poetry to manage python dependencies.  

Install from the `pyproject.toml` file with `poetry install`  

Run django commands like so `poetry run python app/manage.py runserver`  

Secrets under `app/app/.env`  