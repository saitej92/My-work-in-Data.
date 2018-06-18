#### Define schema in pig

pig -useHCatalog

departments = LOAD 'insofe_empdb_mahidharv.departments' USING org.apache.hive.hcatalog.pig.HCatLoader();

dump departments;

======

dept_emp = LOAD 'insofe_empdb_mahidharv.dept_emp' USING org.apache.hive.hcatalog.pig.HCatLoader();

active_dept_emp = FILTER dept_emp BY to_date == '9999-01-01';

active_dept_emp10 = LIMIT active_dept_emp 10;

dump active_dept_emp10;

======

dept_manager = LOAD 'insofe_empdb_mahidharv.dept_manager' USING org.apache.hive.hcatalog.pig.HCatLoader();

active_dept_manager = FILTER dept_manager BY to_date == '9999-01-01';

active_dept_manager10 = LIMIT active_dept_manager 10;

dump active_dept_manager10;

======

employees = LOAD 'insofe_empdb_mahidharv.employees' USING org.apache.hive.hcatalog.pig.HCatLoader();

employees10 = LIMIT employees 10;

dump employees10;

======

salaries = LOAD 'insofe_empdb_mahidharv.salaries' USING org.apache.hive.hcatalog.pig.HCatLoader();

active_salaries = FILTER salaries BY to_date == '9999-01-01';

active_salaries10 = LIMIT active_salaries 10;

dump active_salaries10;

======

titles = LOAD 'insofe_empdb_mahidharv.titles' USING org.apache.hive.hcatalog.pig.HCatLoader();

active_titles = FILTER titles BY to_date == '9999-01-01';

active_titles10 = LIMIT active_titles 10;

dump active_titles10;

################################# Transformations #################################

### Find  active manager for each department
### join departments table with dept_manager to find the current manager for each of the departmetns.

dept_rel = join departments by (dept_no), active_dept_manager by (dept_no);
#dump dept_rel
#describe dept_rel

### Find manager's name from employees table by joining on the emp_no (manager's employee number)
dept_rel2 = join dept_rel by (active_dept_manager::emp_no), employees by (emp_no);
#dump dept_rel2;
#describe dept_rel2;
##dept_rel2: {
##0  dept_rel::departments::dept_no: chararray,
##1  dept_rel::departments::dept_name: chararray,
##2  dept_rel::departments::last_modified: datetime,
##3  dept_rel::active_dept_manager::seq_no: int,
##4  dept_rel::active_dept_manager::dept_no: chararray,
##5  dept_rel::active_dept_manager::emp_no: int,
##6  dept_rel::active_dept_manager::from_date: chararray,
##7  dept_rel::active_dept_manager::to_date: chararray,
##8  dept_rel::active_dept_manager::last_modified: datetime,
##9  employees::emp_no: int,
##10 employees::birth_date: chararray,
##11 employees::first_name: chararray,
##12 employees::last_name: chararray,
##13 employees::gender: chararray,
##14 employees::hire_date: chararray,
##15 employees::last_modified: datetime}

### Remove unwanted columns 
## $0 - dept_rel::departments::dept_no.. 
## Retain dept_no, dept_name, manager_emp_no, manager_from_date, manager_to_date, manager_birth_date, #  
## manager_first_name, manager_last_name, manager_gender, manager_hire_date

final_dept_rel = FOREACH dept_rel2 GENERATE $0 AS dept_no, $1 AS dept_name, $5 AS manager_emp_no, $6 AS manager_from_date, $7 AS manager_to_date, $10 AS manager_birth_date, $11 AS manager_first_name, $12 AS manager_last_name, $13 AS manager_gender, $14 AS manager_hire_date;
dump final_dept_rel;
describe final_dept_rel;

### Find employee and department 
emp_dept_rel = join active_dept_emp by (emp_no), employees by (emp_no);
#describe emp_dept_rel;
## emp_dept_rel: {
##0  active_dept_emp::seq_no: int,
##1  active_dept_emp::emp_no: int,
##2  active_dept_emp::dept_no: chararray,
##3  active_dept_emp::from_date: chararray,
##4  active_dept_emp::to_date: chararray,
##5  active_dept_emp::last_modified: datetime,
##6  employees::emp_no: int,
##7  employees::birth_date: chararray,
##8  employees::first_name: chararray,
##9  employees::last_name: chararray,
##10 employees::gender: chararray,
##11 employees::hire_date: chararray,
##12 employees::last_modified: datetime}

final_emp_dept_rel = FOREACH emp_dept_rel GENERATE $6 AS emp_no, $7 AS birth_date, $8 AS first_name, $9 AS last_name, $10 AS gender, $11 AS hire_date, $2 AS dept_no, $3 AS dept_from_date, $4 AS dept_to_date;

final_emp_dept_rel10 = LIMIT final_emp_dept_rel 10;

dump final_emp_dept_rel10;

## describe final_emp_dept_rel; 


## Find employee and his/her manager's details 
## combine two relations final_emp_dept_rel with the final_dept_rel
active_emp_rel1 = join final_emp_dept_rel by (dept_no), final_dept_rel by (dept_no); 
## describe active_emp_rel1;
## active_emp_rel1: {
##0  final_emp_dept_rel::emp_no: int,
##1  final_emp_dept_rel::birth_date: chararray,
##2  final_emp_dept_rel::first_name: chararray,
##3  final_emp_dept_rel::last_name: chararray,
##4  final_emp_dept_rel::gender: chararray,
##5  final_emp_dept_rel::hire_date: chararray,
##6  final_emp_dept_rel::dept_no: chararray,
##7  final_emp_dept_rel::dept_from_date: chararray,
##8  final_emp_dept_rel::dept_to_date: chararray,
##9  final_dept_rel::dept_no: chararray,
##10 final_dept_rel::dept_name: chararray,
##11 final_dept_rel::manager_emp_no: int,
##12 final_dept_rel::manager_from_date: chararray,
##13 final_dept_rel::manager_to_date: chararray,
##14 final_dept_rel::manager_birth_date: chararray,
##15 final_dept_rel::manager_first_name: chararray,
##16 final_dept_rel::manager_last_name: chararray,
##17 final_dept_rel::manager_gender: chararray,
##18 final_dept_rel::manager_hire_date: chararray}


### Find Salary and Title for each employee
sal_title_rel = join active_titles by (emp_no), active_salaries by (emp_no);
## describe sal_title_rel;
## sal_title_rel: {
##0  active_titles::seq_no: int,
##1  active_titles::emp_no: int,
##2  active_titles::title: chararray,
##3  active_titles::from_date: chararray,
##4  active_titles::to_date: chararray,
##5  active_titles::last_modified: datetime,
##6  active_salaries::seq_no: int,
##7  active_salaries::emp_no: int,
##8  active_salaries::salary: int,
##9  active_salaries::from_date: chararray,
##10 active_salaries::to_date: chararray,
##11 active_salaries::last_modified: datetime}


final_sal_title_rel = FOREACH sal_title_rel GENERATE $1 AS emp_no, $8 AS salary, $9 AS salary_from_date, $10 AS salary_to_date, $2 AS title, $3 AS title_from_date, $4 AS title_to_date;

final_sal_title_rel10 = LIMIT final_sal_title_rel 10;

dump final_sal_title_rel10;

describe final_sal_title_rel;


============================================
active_emp_rel2 = join active_emp_rel1 by (final_emp_dept_rel::emp_no), final_sal_title_rel by (emp_no);
## describe active_emp_rel2;
##   active_emp_rel2: {
##0  active_emp_rel1::final_emp_dept_rel::emp_no: int,
##1  active_emp_rel1::final_emp_dept_rel::birth_date: chararray,
##2  active_emp_rel1::final_emp_dept_rel::first_name: chararray,
##3  active_emp_rel1::final_emp_dept_rel::last_name: chararray,
##4  active_emp_rel1::final_emp_dept_rel::gender: chararray,
##5  active_emp_rel1::final_emp_dept_rel::hire_date: chararray,
##6  active_emp_rel1::final_emp_dept_rel::dept_no: chararray,
##7  active_emp_rel1::final_emp_dept_rel::dept_from_date: chararray,
##8  active_emp_rel1::final_emp_dept_rel::dept_to_date: chararray,
##9  active_emp_rel1::final_dept_rel::dept_no: chararray,
##10 active_emp_rel1::final_dept_rel::dept_name: chararray,
##11 active_emp_rel1::final_dept_rel::manager_emp_no: int,
##12 active_emp_rel1::final_dept_rel::manager_from_date: chararray,
##13 active_emp_rel1::final_dept_rel::manager_to_date: chararray,
##14 active_emp_rel1::final_dept_rel::manager_birth_date: chararray,
##15 active_emp_rel1::final_dept_rel::manager_first_name: chararray,
##16 active_emp_rel1::final_dept_rel::manager_last_name: chararray,
##17 active_emp_rel1::final_dept_rel::manager_gender: chararray,
##18 active_emp_rel1::final_dept_rel::manager_hire_date: chararray,
##19 final_sal_title_rel::emp_no: int,
##20 final_sal_title_rel::salary: int,
##21 final_sal_title_rel::salary_from_date: chararray,
##22 final_sal_title_rel::salary_to_date: chararray,
##23 final_sal_title_rel::title: chararray,
##24 final_sal_title_rel::title_from_date: chararray,
##25 final_sal_title_rel::title_to_date: chararray}

active_employees_data = FOREACH active_emp_rel2 GENERATE $0 AS emp_no, $2 AS first_name, $3 AS last_name, $4 AS gender, $1 AS birth_date, $5 AS hire_date, $6 AS dept_no, $10 AS dept_name, $7 AS dept_from_date, $20 AS salary, $21 AS salary_from_date, $23 AS title, $24 AS title_from_date, $11 AS manager_emp_no, $15 AS manager_first_name, $16 AS manager_last_name, $17 AS manager_gender, $14 AS manager_birth_date, $18 AS manager_hire_date, $12 AS manager_from_date, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($1,'yyyy-MM-dd')) AS age, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($5,'yyyy-MM-dd')) AS tenure, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($14,'yyyy-MM-dd')) AS manager_age, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($18,'yyyy-MM-dd')) AS manager_tenure, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($21,'yyyy-MM-dd')) AS salary_since, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($24,'yyyy-MM-dd')) AS role_since;

STORE active_employees_data INTO '/user/1368B30/datasets/employeesdb/active_employees_data/' USING PigStorage(',');

Fields:
 emp_no, 
 first_name, 
 last_name, 
 gender, 
 birth_date, 
 hire_date, 
 dept_no, 
 dept_name, 
 dept_from_date, 
 salary, 
 salary_from_date, 
 title, 
 title_from_date, 
 manager_emp_no, 
 manager_first_name, 
 manager_last_name, 
 manager_gender, 
 manager_birth_date, 
 manager_hire_date, 
 manager_from_date, 
 age,
 tenure,
 manager_age,
 manager_tenure,
 salary_since,
 role_since