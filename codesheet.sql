
--Task 1---  Create Role Hierarchy  ----

use role accountadmin;
create role admin;
create role developer;
grant role developer to role admin;
grant role admin to role accountadmin;
create role pii;
grant role pii to role accountadmin;


--Task 2  ---- Create Data warehouse of M-Size

use role accountadmin;
CREATE or REPLACE WAREHOUSE assignment_wh WITH WAREHOUSE_SIZE = 'Medium'
auto_suspend = 60
auto_resume =true;
grant usage on warehouse assignment_wh to role admin;


--Task 3  ---- Switch to Role Admin

grant create database on account to role admin;
use role admin;


--Task 4  ----  Create a Database

CREATE or REPLACE TRANSIENT DATABASE assignment_db;
USE DATABASE assignment_db;


--Task 5  ----  Create a Schema

CREATE or REPLACE SCHEMA my_schema;

--Task 6  ----  Create tables to load Data

create or replace table assignment_db.my_schema.my_int_employee(
         file_name varchar,
         elt_by varchar,
         elt_ts timestamp,
         employee_id INT not null,
         first_name string ,
         last_name string ,
         email string ,
         phone_number STRING,
         hire_date date,
         job_id string,
         salary INT,
         manager_id int,
         department_id INT
         
);


CREATE or REPLACE TABLE my_ext_employee LIKE assignment_db.my_schema.my_int_employee;


--Task 7  ----  Create Varient version Dataset

create or replace table variant_dataset (
 data1 variant
);


insert into variant_dataset 
 (select to_variant(object_construct(*)) as data1 from my_int_employee limit 10);

select * from variant_dataset;


--Task 8  ----  Create and Load Data into Internal Stage and External Stage

create or replace stage assignment_db.my_schema.my_int_stage;

create or replace file format assignment_db.my_schema.my_csv_format
type = csv field_optionally_enclosed_by='"' field_delimiter = ',' 
SKIP_HEADER = 1
null_if = (' - ', 'NULL', 'null') empty_field_as_null = true
date_format = 'DD-MM-YY';


create storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::407784018556:role/snowflake_role'
  storage_allowed_locations = ('s3://snowflakeproject123/emp/');

DESC INTEGRATION s3_int;

create or replace stage assignment_db.my_schema.my_s3_stage
  storage_integration = s3_int
  url = 's3://snowflakeproject123/emp/'
  file_format = assignment_db.my_schema.my_csv_format;


--Task 9  ----    Copy Data from Stage to Snowflake Table


copy into assignment_db.my_schema.my_int_employee
from (select metadata$filename,'local',current_timestamp(),t.$1 , t.$2 , t.$3 , t.$4 , t.$5 , to_varchar(t.$6::date,'DD-MM-YY') ,t.$7,t.$8,t.$10,t.$11 from @assignment_db.my_schema.my_int_stage t)
file_format = assignment_db.my_schema.my_csv_format
on_error = 'skip_file';


select * from my_int_employee;


copy into assignment_db.my_schema.my_ext_employee
from (select metadata$filename,'AWS S3',current_timestamp(),t.$1 , t.$2 , t.$3 , t.$4 , t.$5 , to_varchar(t.$6::date,'DD-MM-YY'),t.$7,t.$8,t.$10,t.$11 from @assignment_db.my_schema.my_s3_stage/ t)
file_format = assignment_db.my_schema.my_csv_format
on_error = 'skip_file';

select * from assignment_db.my_schema.my_ext_employee;


--Task 10  -----   Upload any unrelated parquet file to the stage location and infer the
-- schema of the file

create file format my_parquet_format
  type = parquet;
  
create stage parquet_stage
 file_format = my_parquet_format ;
  

select *
  from table(
    infer_schema(
      location=>'@parquet_stage'
      , file_format=>'my_parquet_format'
      )
    );


-- Task 11  ----  Query on Staged Parquet File

select * from @ASSIGNMENT_DB.MY_SCHEMA.parquet_stage;


-- Task 12  ---- Adding Masking Policy  

create or replace masking policy assignment_db.my_schema.email_mask as (val string) returns string ->
  case
    when current_role() in ('DEVELOPER') then '******'
    else val
  end;

GRANT SELECT ON TABLE assignment_db.my_schema.my_int_employee TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.my_int_employee TO ROLE PII;

GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA my_schema TO ROLE Developer;

GRANT USAGE ON WAREHOUSE ASSIGNMENT_WH TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA my_schema TO ROLE PII;

alter table if exists assignment_db.my_schema.my_int_employee modify column email set masking policy 
assignment_db.my_schema.email_mask;

alter table if exists assignment_db.my_schema.my_int_employee modify column phone_number set masking policy 
assignment_db.my_schema.email_mask;

Use role developer;
select * from assignment_db.my_schema.my_int_employee;


