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
    which will be really useful to further evaluate the potential of the new service and understand what are its drivers of growth. 
    This models can be easily used to create visualizations given that curated data has been aggregated per customer in one model and per date in the daily aggregations model, which allows for daily, weekly and monthly analysis of almost any service and customer metric I could think of.

# What I would do if I had more time
- create a model that would have the status changes for all the orders to be able to track the timestamps of each of these for better CX or further internal operational optimizations
- Add other relevant metrics to the daily aggregations model. For now its limited to just rolling metrics for revenue tracking
- create the DAGs for ingestion
- create an actual incremental setup from the staging layer onwards for scalability. At the moment, incremental configuration is done in each individual model, which means the entire "FROM" table has to be scanned before being filtered.
- make a dashboard in tableau or looker with the customer aggegations or daily aggregations data (assuming more data volume)

## Ideal CI/CD Process with unlimited resources
Assuming I have the resources and time to build a really good CI/CD process to ensure data quality and smooth collaboration it would include the following: I would introduce a staging environment with a replica of the entire datasets (assuming no opportunity cost for having 2 completely identical full datasets) to monitor recently deployed changes before going to production. The CI would include not only build and test the modified and children models, but also check for correct linting with SQLfluff and accurate documentation, ensuring not only the use of already existing documentation but also validating congruency between yml and sql files. For deployment, I would set up an automatic staging deployment on merges to main and then for prod scheduled or manual depending on data deployment regulations. To close the loop, I would add model run performance monitoring to always look out for possible bottle necks / optimizations and also slack alerts to ensure quick response on prod run failures. For tools I would stick to github actions and airflow for orchestration

## CI/CD in real world with limited resources

When I first joined SumUp, I noticed that analytics engineers in Chile worked on separate teams with their own projects and repositories, maintained by many other engineers. We had a local project for Latam specific data models, but since it wasn't anyone's main priority, there were no guidelines or best practices for collaborative work. No one cared if code was clean, reviews were non-existent, and PR approvals were given without actually checking the code being merged. This was clearly an issue, but since it wasn't anyone's top priority, nothing was done about it. To fix this, I implemented a quick win: created essential tests for existing dbt models and configured simple CI with GitHub Actions that ensured code changes were tested with each pull request. Even though this wasn't a huge project, it was low effort for high benefits: a cleaner collaboration ecosystem that resulted in fewer production failures. From my experience where we had collaboration issues, I would start with the absolute basics that give maximum impact for as little effort assuming time is also a limited resource. So I would make code reviews mandatory before merging and set up basic github actions CI that runs dbt run and dbt test on every PR, the simple tests for critical models like not_null or unique for key columns, etc. I would also add code formatting checks to keep a standard between collaborators and I would also create a separate staging schema in the same database for testing changes.

For longterm, I would implement proper staging environment in a separate database. Add comprehensive data quality tests and set up slack alerts for production failures. I would also consider setting up the CI to use a sample of the data instead of the full dataset just to improve efficiency: No one likes to make simple changes to code in 5 minutes and then waiting 10 minutes for the CI to run fully.

The good thing is that even basic CI implementation prevents most collaboration problems with very little setup time. A simple github actions workflow that prevents broken code from reaching production is much better than having nothing at all. From there you can build on top of it and make it more sophisticated, but to start off a simple setup does a lot of good to a team of analytics engineers collaborating on the same project.