# air-bolt
This project aims to simulate the ideal pipeline and data transformation + analytical model creation for the Air Boltic service.
This project contains files for sources, staging, intermediate and mart models. Alongside an air_boltic.md file with some field documentation.

Even though models were entirely built from scratch with no aid, AI was used to help me translate some functions from Snowflake SQL to Databricks SQL syntaxis. 

# NOTE:

1. The entire project is built assuming only availability for the provided data sources
    - customers
    - customer_groups
    - trips
    - airplanes
    - airplane_models
    - orders

2. The YAML file in the sources folder specifies all of the previously mentioned sources
    *IMPORTANT* 
    This YAML file is the only .yml file in the project with actual content (for time saving purposes) and it includes the following
    1. Table names and column name + descriptions
    2. Referenced {{ docs }} for some fields. This content can be viewed in the docs folder
    3. dbt tests

3. This dbt project not only has enriched data for each of the data sets provided but also customer and daily aggregation models 
    which will be really useful to further evaluate the potential of the new service and understand what are its drivers of growth

# What I would do if I had more time
- create a model that would have the status changes for all the orders to be able to track the timestamps of each of these for better CX or further internal operational optimizations
- create the DAGs for ingestion
- create an actual incremental setup from the staging layer onwards for scalability. At the moment, incremental configuration is done in each individual model, which means the entire "FROM" table has to be scanned before being filtered.

