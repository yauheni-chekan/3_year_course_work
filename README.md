# Vehicle Rental Data Warehouse Project

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Overview](#project-overview)
- [Database Schemas](#database-schemas)
- [Installation Steps](#installation-steps)
- [ETL Process Creation](#etl-process-creation)
- [Get Insights](#get-insights)
- [PowerBI Dashboard](#powerbi-dashboard)

## Prerequisites

- PostgreSQL 17 or higher
- PowerBI Desktop
- Access to command line/terminal
- Sample data files (provided in `sample_data` folder)

## Project Overview

This project implements a data warehouse for a vehicle rental business. It consists of:

- OLTP database (vehicle_rental) for transactional data
- OLAP database (vehicle_rental_dwh) for analytics
- SQL scripts to load sample data into the OLTP database
- Foreign Data Wrapper (FDW) to connect to the OLTP database
- ETL process to move data between databases
- SQL queries to get insights from the OLTP and OLAP databases
- PowerBI dashboard for visualization

## Database Schemas

### OLTP Schema (vehicle_rental)

- Tables and Relationships

![alt text](<Vehicle Rental OLTP DB ERD.png>)

### OLAP Schema (vehicle_rental_dwh)

![alt text](<Vehicle Rental OLAP DB ERD.png>)

#### Dimension Tables

1. dim_customer
  - PK: customer_id
  - Tracks customer information
  - SCD Type 2 enabled with last_modified
2. dim_vehicle
  - PK: vehicle_id
  - Tracks vehicle inventory
  - SCD Type 2 enabled with last_modified
3. dim_employee
  - PK: employee_id
  - Tracks employee information and their role in the rental transactions
  - SCD Type 2 enabled with last_modified
4. dim_date
  - PK: date_id
  - Tracks date information and its associated details
5. dim_branch
  - PK: branch_id
  - Tracks branch information and its location
  - SCD Type 2 enabled with last_modified
6. dim_vehicle_type
  - PK: vehicle_type_id
  - Tracks vehicle type information and its attributes
  - SCD Type 2 enabled with last_modified

#### Fact Tables

1. fact_rental_transactions
  - PK: rental_transaction_id
  - Tracks rental transactions and their associated details
2. fact_maintenance
  - PK: maintenance_id
  - Tracks maintenance records for vehicles and their associated details

#### Control Tables

1. etl_control
  - Tracks the status of the ETL process

## Installation Steps

### 1. Create OLTP Database

Create the OLTP database using the following command.
```bash
$ psql -U postgres -c "CREATE DATABASE vehicle_rental;"
```
Alternatively, you can create the database in PGAdmin.
```text
- Open pgAdmin
- Connect to your server
- Click on the "Databases" tab
- Click the "Create" button
- Enter the database name: vehicle_rental
- Click "Save"
```

### 2. Create database structure running the script from `create_car_rental_db_structure.sql` file using the following command.
```bash
$ psql -U postgres -f C:/path/to/create_car_rental_db_structure.sql
```
Alternatively, you can open the file in PGAdmin and run the script from there.
```text
- Open pgAdmin
- Connect to your server
- Click on the "Query Tool" button
- Click the "Open File" button (folder icon)
- Navigate to and select `create_car_rental_db_structure.sql` SQL file
- Click "Execute" (or press F5)
```

### 3. Create database functions running the script from `oltp_data_import_scripts.sql` file.
```bash
$ psql -U postgres -f C:/path/to/oltp_data_import_scripts.sql
```
Alternatively, you can open the file in PGAdmin and run the script from there. Use the steps mentioned in the previous section.

### 4. Import OLTP Data from CSV files

**NOTE:** Make sure PostgreSQL has access to all the csv data files in `sample_data` folder.
If not, place the files in the folder that PostgreSQL has access to. (e.g. `"C:\Program Files\PostgreSQL\17\data"`)

Run the following commands in the Query Tool to load the sample data from csv files.
```sql
-- Load the sample data from csv files.
SELECT load_customer_data('/path/to/local/customer.csv');
SELECT load_vehicle_data('/path/to/local/vehicle.csv');
SELECT load_transactions_from_csv('/path/to/local/transactions.csv');
SELECT load_payments_from_csv('/path/to/local/payments.csv');
SELECT load_maintenance_from_csv('/path/to/local/maintenance.csv');
```

### 5. Create OLAP Database

Create the OLAP database using the following command.
```bash
$ psql -U postgres -c "CREATE DATABASE vehicle_rental_dwh;"
```
Alternatively, you can create the database in PGAdmin.
```text
- Open pgAdmin
- Connect to your server
- Click on the "Databases" tab
- Click the "Create" button
- Enter the database name: vehicle_rental_dwh
- Click "Save"
```

### 6. Create OLAP Database Structure

Run the following command in the Query Tool to create the OLAP database structure.
```bash
$ psql -U postgres -f C:/path/to/create_car_rental_dwh_structure.sql
```
Alternatively, you can open the file in PGAdmin and run the script from there. Use the steps mentioned in the previous section.

### ETL Process Creation

In OLAP database, create the ETL process by running the following commands.
```text
- Open pgAdmin
- Connect to your server
- Click on the "Query Tool" button choosing the OLAP database
- Click the "Open File" button (folder icon)
- Navigate to and select `etl_scripts.sql` SQL file
- Click "Execute" (or press F5)
```

**NOTE:** This script will use the Foreign Data Wrapper to connect to the OLTP database and load the data.
The subsequent steps can be done from the OLAP database.

### 8. Load OLTP Data

Run the ETL process created in the previous step to load the OLTP data into the OLAP database.
```sql
-- Run the ETL process.
SELECT run_etl();
```

## Get Insights

The provided `sql_insights_scripts.sql` file contains SQL queries to get insights from the OLTP and OLAP databases.

The queries should be run from the OLAP database.

Other queries can be added to the file to get more insights if needed.

## PowerBI Dashboard

Open the provided `vehicle_rental_analytics.pbix` file in PowerBI Desktop.

The data model is already created in the file.

There are 2 pages in the report:
- `Rentals` - Shows the rental analytics data.
- `Maintenance` - Shows the maintenance analytics data.
