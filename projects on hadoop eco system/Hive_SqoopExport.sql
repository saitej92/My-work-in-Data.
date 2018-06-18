#########################################################################################
 
Objective: Define hive table for the previously created/written data in hdfs by pig
and create two aggregate files into HDFS using Hive
1. Based on department
2. Based on department and gender

#### Create external table for the above large table with all the information of all active employees.

DROP DATABASE IF EXISTS employees_db_1368B30;

CREATE DATABASE IF NOT EXISTS employees_db_1368B30;

USE employees_db_1368B30;

DROP TABLE IF EXISTS active_emp_details;

CREATE EXTERNAL TABLE IF NOT EXISTS active_emp_details(
        emp_no                  INT,
        first_name              STRING,
        last_name               STRING,
        gender                  CHAR(1),
        birth_date              DATE,
        hire_date               DATE,
        dept_no                 CHAR(4),
        dept_name               STRING,
        dept_from_date          DATE,
        salary                  INT,
        salary_from_date        DATE,
        title                   STRING,
        title_from_date         DATE,
        manager_emp_no          INT,
        manager_first_name      STRING,
        manager_last_name       STRING,
        manager_gender          CHAR(1),
        manager_birth_date      DATE,
        manager_hire_date       DATE,
        manager_from_date       DATE,
        age                     INT,
        tenure                  INT,
        manager_age             INT,
        manager_tenure          INT,
        salary_since            INT,
        role_since              INT)
    COMMENT 'Data about all active employees derived from employees database'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/1368B30/datasets/employeesdb/active_employees_data/';
   


======================== CREATE INTERMEDIATE TABLES ==============================
#INSERT OVERWRITE LOCAL DIRECTORY '/home/1368B30/Desktop/hive_output/'
#CREATE TABLE dept_stats1 AS
#### From the above external table - create two aggregate tables
#### 1. By Department and By Gender with all the totals, averages etc.
#### 2. By Department with all the totals, averages etc.
#### and write those results as files into HDFS

INSERT OVERWRITE DIRECTORY '/user/1368B30/datasets/employeesdb/dept_aggr_by_gender/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT 
dept_no AS dept_no,
dept_name AS dept_name,
gender AS gender,
count(*) AS tot_emp,
sum(salary) AS tot_sal,
min(salary) AS min_sal,
max(salary) AS max_sal,
avg(salary) AS avg_sal,
min(age) AS min_age,
max(age) AS max_age,
avg(age) AS avg_age,
min(tenure) AS min_tenure,
max(tenure) AS max_tenure,
avg(tenure) AS avg_tenure,
avg(salary_since) AS avg_sal_change,
avg(role_since) AS avg_role_change
FROM active_emp_details
GROUP BY dept_no, gender, dept_name;


INSERT OVERWRITE DIRECTORY '/user/1368B30/datasets/employeesdb/dept_aggr/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT 
dept_no AS dept_no,
dept_name AS dept_name,
count(*) AS tot_emp,
sum(salary) AS tot_sal,
min(salary) AS min_sal,
max(salary) AS max_sal,
avg(salary) AS avg_sal,
min(age) AS min_age,
max(age) AS max_age,
avg(age) AS avg_age,
min(tenure) AS min_tenure,
max(tenure) AS max_tenure,
avg(tenure) AS avg_tenure,
avg(salary_since) AS avg_sal_change,
avg(role_since) AS avg_role_change
FROM active_emp_details
GROUP BY dept_no, dept_name;
#############################################################################################

Objective: Define the schema/ create mysql tables for the large database and aggregate tables.
#### Create the database and tables in MySQL to load the results.

CREATE DATABASE IF NOT EXISTS insofe_results_saitejgv_saitejgv;
use insofe_results_saitejgv_saitejgv;
CREATE TABLE IF NOT EXISTS active_emp_details (
emp_no INT NOT NULL,
first_name VARCHAR(14) NOT NULL,
last_name VARCHAR(16) NOT NULL,
gender CHAR(1) NOT NULL,
birth_date DATE NOT NULL,
hire_date  DATE NOT NULL,
dept_no CHAR(4) NOT NULL,
dept_name VARCHAR(40) NOT NULL,
dept_from_date DATE NOT NULL,
salary INT NOT NULL,
salary_from_date DATE NOT NULL,
title VARCHAR(50) NOT NULL,
title_from_date DATE NOT NULL,
manager_emp_no INT NOT NULL,
manager_first_name VARCHAR(14) NOT NULL,
manager_last_name VARCHAR(16) NOT NULL,
manager_gender CHAR(1) NOT NULL,
manager_birth_date DATE NOT NULL,
manager_hire_date DATE NOT NULL,
manager_from_date DATE NOT NULL,
age  TINYINT NOT NULL,
tenure TINYINT NOT NULL,
manager_age TINYINT NOT NULL,
manager_tenure TINYINT NOT NULL,
salary_since TINYINT NOT NULL,
role_since TINYINT NOT NULL,
KEY (emp_no),
PRIMARY KEY (emp_no)
);
        
        
CREATE TABLE IF NOT EXISTS dept_aggr_by_gender (
        dept_no                 CHAR(4)         NOT NULL,
        dept_name               VARCHAR(40)     NOT NULL,
        gender                  CHAR(1)         NOT NULL,
        tot_emp                 BIGINT          NOT NULL,
        tot_sal                 BIGINT          NOT NULL,
        min_sal                 BIGINT          NOT NULL,
        max_sal                 BIGINT          NOT NULL,
        avg_sal                 DECIMAL(20,2)   NOT NULL,
        min_age                 TINYINT         NOT NULL,
        max_age                 TINYINT         NOT NULL,
        avg_age                 DECIMAL(5,2)    NOT NULL,
        min_tenure              TINYINT         NOT NULL,
        max_tenure              TINYINT         NOT NULL,
        avg_tenure              DECIMAL(5,2)    NOT NULL,
        avg_sal_change          DECIMAL(5,2)    NOT NULL,
        avg_role_change         DECIMAL(5,2)    NOT NULL,
        KEY (dept_no),
        KEY (gender),
        PRIMARY KEY (dept_no, gender));
        
CREATE TABLE IF NOT EXISTS dept_aggr (
        dept_no                 CHAR(4)         NOT NULL,
        dept_name               VARCHAR(40)     NOT NULL,
        tot_emp                 BIGINT          NOT NULL,
        tot_sal                 BIGINT          NOT NULL,
        min_sal                 BIGINT          NOT NULL,
        max_sal                 BIGINT          NOT NULL,
        avg_sal                 DECIMAL(20,2)   NOT NULL,
        min_age                 TINYINT         NOT NULL,
        max_age                 TINYINT         NOT NULL,
        avg_age                 DECIMAL(5,2)    NOT NULL,
        min_tenure              TINYINT         NOT NULL,
        max_tenure              TINYINT         NOT NULL,
        avg_tenure              DECIMAL(5,2)    NOT NULL,
        avg_sal_change          DECIMAL(5,2)    NOT NULL,
        avg_role_change         DECIMAL(5,2)    NOT NULL,
        PRIMARY KEY (dept_no));
############################################################################################

Objective: export data from HDFS to MySQL using Sqoop.

#### Export from HDFS to MySQL using sqoop export        
        172.16.0.249/insofe_employees_1368B30
sqoop job --create export_all -- export --connect jdbc:mysql://172.16.0.249/insofe_results_saitejgv_saitejgv --username insofeadmin --password Insofe_passw0rd --table active_emp_details --export-dir '/user/1368B30/datasets/employeesdb/active_employees_data/' --batch --update-key emp_no --update-mode allowinsert

sqoop job --exec export_all

sqoop job --create export_aggr_2 -- export --connect jdbc:mysql://172.16.0.249/insofe_results_saitejgv --username insofeadmin --password Insofe_passw0rd --table dept_aggr_by_gender --export-dir '/user/1368B30/datasets/employeesdb/dept_aggr_by_gender/' --batch --update-key dept_no,gender --update-mode allowinsert

sqoop job --exec export_aggr_2

sqoop job --create export_aggr_3 -- export --connect jdbc:mysql://172.16.0.249/insofe_results_saitejgv --username insofeadmin --password Insofe_passw0rd --table dept_aggr --export-dir '/user/1368B30/datasets/employeesdb/dept_aggr/' --batch --update-key dept_no --update-mode allowinsert

sqoop job --exec export_aggr_3