-- Create Temp Table To insert the casted column from the original table
CREATE OR REPLACE TABLE `resolute-choir-403411.dbt_production.yellow_temp`  AS
SELECT cast(payment_type as INT64) as payment_type FROM `resolute-choir-403411.dbt_production.yellow` ;

-- Compare the casted column that i inserted in another table to the original one
select payment_type , count(*) from `resolute-choir-403411.dbt_production.yellow`
group by payment_type ;

select payment_type , count(*) from `resolute-choir-403411.dbt_production.yellow_temp`
group by payment_type ;

-- This is How to drop column in bigquery if the above comparison is correct
CREATE OR REPLACE TABLE `resolute-choir-403411.dbt_production.yellow`AS
SELECT * EXCEPT (payment_type) FROM  `resolute-choir-403411.dbt_production.yellow`

-- Create An Empty column with the new data type 
ALTER TABLE `resolute-choir-403411.dbt_production.yellow`
ADD COLUMN payment_type INT64;

-- Insert The dropped column from the temp table 
INSERT INTO `resolute-choir-403411.dbt_production.yellow` (payment_type)
    SELECT 
      payment_type
    FROM
        `resolute-choir-403411.dbt_production.yellow_temp`

-- Loading Into Table From Parquet File In Buckets
LOAD DATA INTO `resolute-choir-403411.dbt_production.yellow`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://yellow_trip_zc/data/yellow/yellow_tripdata_2019-06.parquet']);


