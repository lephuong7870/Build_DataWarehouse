use role accountadmin;
CREATE WAREHOUSE project_wh with warehouse_size='x-small';
CREATE DATABASE  IF NOT EXISTS project_db;
CREATE ROLE IF NOT EXISTS project_role ;

GRANT ROLE project_role to user lephuong7870;
GRANT USAGE ON WAREHOUSE project_wh TO ROLE project_role ;
GRANT ALL ON DATABASE project_db TO ROLE project_role ;

CREATE SCHEMA IF NOT EXISTS project_db.project_schema ;



-- create stage

CREATE OR REPLACE STAGE project_db.project_schema.csv_local ;
SHOW STAGES;

CREATE OR REPLACE FILE FORMAT  allow_duplicate_ff type='csv' field_delimiter=',' skip_header=1 ; 
SHOW FILE FORMATS ;