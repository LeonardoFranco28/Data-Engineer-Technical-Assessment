# **Case Definition**

A telecommunications company operates across multiple countries and collects daily location data from millions of customers through its mobile network. The business intelligence team needs to understand customer proximity to points of interest (POIs) tagged with specific event types, in order to support targeted marketing campaigns, network capacity planning, and customer segmentation.

Two datasets are available: a geo table with customer location coordinates and commune, and a labels table with customers linked to specific event types (type 1 and type 2). Both datasets are generated daily. As part of your solution, you must define how and where this data will be ingested and stored, in what format, and how it will be partitioned to support efficient historical queries.

The core analytical challenge is identifying, for each customer, which labeled events occur within a 50-meter radius. Given the volume of data (millions of records per day), a naive approach would generate an unmanageable number of combinations. Your solution must address this scalability problem.

The processed output must serve two audiences: Data Scientists who work in Jupyter notebooks and require a rich feature table, and Business Analysts who query data through SQL. Your architecture and output schema should account for both access patterns.

**You must submit your solution within 7 calendar days of receiving this test. Please send all deliverables (architecture diagram, PySpark script, and completed notebook) as a single compressed file or shared repository link.**

# **Exercise**

## **Point 1: (Architecture)**

Before defining the technical components, write a short planning document that covers: (1) the main risks and constraints you identified in the case, (2) your design principles (e.g. cost, scalability, simplicity), and (3) why you chose AWS, GCP, or a hybrid approach. Then, based on that plan, define the following components of your architecture. The design must support adding similar data sources in future versions:

* Storage tools and layers (Raw, Staging, Analytics, Curated): define the storage location for each layer, the file format, and the partitioning strategy that supports daily ingestion and efficient historical queries.  
* Tools for transforming the data (choose and justify your selection; options include: AWS Glue, EMR \+ Apache Spark, Databricks, Google Dataflow, Dataproc. If you choose Databricks or Snowflake, explain how it fits the cost-effectiveness requirement.)  
* Define the orchestration tool (e.g. Apache Airflow, AWS Step Functions, Prefect, Dagster) and justify why it fits this pipeline.  
* Define the notification and alerting tool (e.g. Airflow callbacks, AWS SNS, PagerDuty) and describe what conditions should trigger an alert.  
* Define the tool for Data Scientists to access the output (e.g. SageMaker, AI Platform, Databricks notebooks, EC2 with JupyterHub) and explain how they will query the data.  
* Define the tool for Business Analysts to query the data via SQL (e.g. Amazon Athena, Redshift, BigQuery, Snowflake, Databricks SQL, Hive) and explain how the output table will be exposed to them.  
* Define the storage format for each layer (e.g. Parquet, Delta Lake, Apache Iceberg) and justify the choice in terms of query performance and cost.

## **Point 2: (ETL Script)**

Develop a PySpark, Spark or Scala script to process the sample (500K approx.) provided for this exercise, remember to create a script as efficiently as possible because this should work with full data (millions of records).

**The sample data is available in a shared Google Drive, in parquet files format in two different folders and structures; geo and labels, the following images show the table structures and a code example for importing the data into a Google Colab environment**

Consider the following points to develop the script. Each step should be implemented in order:

* The ETL process can be developed in the provided Colab notebook template, but a standalone script is required as a final delivery.  
* Data profiling and cleaning: before any transformation, inspect both tables and document your findings. This must include: (a) row counts and schema validation for each table; (b) detection and handling of NULL values in key fields (ID, comuna, event, latitude, longitude); (c) detection of out-of-range or anomalous coordinate values; (d) identification of duplicate IDs and your chosen deduplication strategy with justification. Add a brief comment block summarizing what you found and what you did about it.  
* Join the datasets: once the data is clean, join labels and geo through the ID field. Document the join type chosen (inner, left, etc.) and justify it based on your profiling findings — particularly the ID overlap between tables.  
* Deduplication: as part of the cleaning step, eliminate duplicate IDs. For the geo table keep the first value of comuna, latitude and longitude. For the labels table, aggregate the event field. Explain in a comment why you chose "first" and whether other strategies (e.g. most frequent, most recent) would be more appropriate for this use case.  
* Calculate the distance (it can be Euclidean, Manhattan or any other method) between all clients in 50 meters around.  
* Define and implement an optimization strategy to handle the combinatorial explosion of the distance calculation. In a brief comment block (5–10 lines) within your script, explain: (1) what strategy you chose, (2) why it is appropriate for this data, and (3) what the trade-offs are compared to at least one alternative approach.  
* Define the output table schema, this may be different for data scientist and data analysts depending on the architecture that you define in point 1\.  
* Save the output of the previous point thinking of having a weekly history in a file with the indicated format.

## **Point 3: (Analytics & Data Quality)**

Connect the Colab notebook to a SQLite database to validate data quality and make the results available for Data Scientists and BI Analysts. For each query below, briefly comment on what business question it answers and what data quality issue it could reveal.

* Load the labels table and the result table from Point 2 into SQLite (the notebook contains an example with the geo table).  
* Data Quality checks: write queries to (a) count rows with NULL values in any key field (ID, comuna, event); (b) detect duplicate IDs after deduplication; (c) find customers whose coordinates fall outside the expected range for their commune (use a subquery to compute the commune bounding box, then flag outliers).  
* Using a window function (RANK or DENSE\_RANK), rank communes by the total number of type 2 events and return the top 20\. Include a second column showing each commune's share (%) of the national total for type 2 events (use a subquery or CTE for the national total).  
* Using a CTE, calculate for type 1 events by commune: avg, max and min of latitude and longitude, the event count, and the distance between the max and min latitude points. Then, in the outer query, return only communes where the avg latitude deviates more than 10% from the overall national average (this tests awareness of geographic outliers).  
* For each commune, use a single query with conditional aggregation (no JOINs) to show side-by-side: count of type 1 events, count of type 2 events, and the ratio type\_2 / type\_1. Order by ratio descending. This simulates a typical BI analyst request on the final output table.