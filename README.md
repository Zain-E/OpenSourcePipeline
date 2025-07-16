# **OPEN-SOURCE-PIPELINE**

This repo will ingest data from numerous different sources into an aws account and then model the data
through a data warehouse and analytical layer.  The purpose of this project is:

1. To create a cost-efficient pipeline that can be replicated at scale
2. To utilise non-standard practices that may be better than the oft-repeated "best practice"
3. To use open-source tools (that are also scalable) as much as possible to compliment point 1. [dlthub](https://dlthub.com/), [dagster](https://dagster.io/community), [SQLMesh](https://sqlmesh.readthedocs.io/en/stable/integrations/dlt/) (and perhaps other services like dagster), and [AWS](https://aws.amazon.com/?nc2=h_lg) are services that will be used in this repo, which can be used as a template for future work.


## dltHub Developer Notes

<img src="assets/dlthub-logo.png" alt="Description" width="300" height="100"/>

- There still needs to be work to speed up pipeline for large datasets - works fine for tables <100,000 rows or APIs
- Set up your credentials in ``` .dlt/secrets.toml``` - in this case they are s3 + GoogleBigQuery account credentials
- If you have added env variables to a `.env` file, use the ```source .env``` command to load them into shell.
- Run the command ```./run_ingest``` (locally) to run all the ingest pipelines (you can also run them individually if needed per pipeline).
- The file ```xxx``` contains dlt run artifacts for observability.

## Dagster Developer Notes

![dagster](assets/dagster_icon.png)



## SQLMesh Developer Notes

![sqlmesh](assets/sqlmesh_icon.png)

- Use of virtual environments is recommended, especially if you work on multiple client/internal projects requiring conflicting dbt or Python versions
- Set up your local profiles.yml, you will need to direct the ./run filepath to the location of your profiles.yml & dbt_project.yml files (lines 3 + 4):
 ``` $ export DBT_PROFILES_DIR=path/to/directory``` & ```$ export DBT_PROJECT_DIR=path/to/directory```.
- If you have added env variables to a `.env` file, use the ```source .env``` command to load them into shell.
- If running dbt from the command line, you will need to navigate ```cd folder/filepath``` into the landnerds folder. 
- Run dbt debug to check your project setup and authentication is correct.
- profiles.yml has 2 targets, **dev** & **prod**, and by default is set to '**prod**', set to '**dev**' for local use.
- Run the command ```./run``` (locally) for ease; this script sets ```profiles.yml & dbt_project.yml``` filepaths (which will need amending for PROD) runs ```dbt compile```, ```dbt run``` & ```dbt docs``` with 1 simple command.
- The table ```landnerds.2_data_warehouse.observability``` contains dbt run artifacts for observability.

## AWS folder structure

<img src="assets/aws-white.png" alt="Description" width="150" height="150"/>

The folder structure used in this project is aligned to general best practices with a number of layers, a staging (ephemeral) layer,  warehouse (table) layer, analytics (table) layer and semantics (view) layer.  Depending on the tool, the final layer may not be necessary but have been added as Looker Data Studio has minimal modelling capabilites. 

The structure is separated into distinct layers of transformation:
- Staging
- Warehouse
- Analytics
- Semantics

## Data structure

The data structure within the warehouse layer is generally agnostic, and for the analytics layer follows a snowflake dim/fact schema (with conformed dimensions).  ***Please note, not all tables have been modelled, in the interest of time, modelling has been limited to relevant tables***  


## Dashboard

<img src="assets/streamlit_red.svg" alt="Description" width="150" height="150"/>

- Visuals use streamlit (for now) to present the yugioh data.
- Run the command ```./run_visualise``` (locally) to run streamlit and display the dashboard on http://localhost:8502/


## Linters and other tools

SQLFluff is used for linting and checking of code quality.  It is recommended to install and run SQLFluff locally prior to commiting your code to accelerate the development process. The rules enforced by SQLFluff can be found in the .sqlfluff file.

Within landnerds:

Run the command ```sqlfluff lint models/*``` and then ```sqlfluff fix models/*``` to fix all models wihin the models folder.

**Note:** when running SQLFluff from the command line, you must run it from 'landnerds'. This ensures that the rules applied in the .sqlfluff file are selected rather than the default rules.  
