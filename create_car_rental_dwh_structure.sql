-- SQL Script to Create DWH Tables
-- The database is named 'vehicle_rental_dwh' and is owned by the 'postgres' user.
-- =======================================================================================

-- Dimension Tables
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone_number VARCHAR(15),
    address TEXT,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_active BOOLEAN DEFAULT TRUE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_branch (
    branch_id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(255) NOT NULL,
    contact_number VARCHAR(15),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_active BOOLEAN DEFAULT TRUE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_employee (
    employee_id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone_number VARCHAR(15),
    branch_id INT NOT NULL,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_active BOOLEAN DEFAULT TRUE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_vehicle_type (
    vehicle_type_id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    type_name VARCHAR(50) NOT NULL,
    description TEXT,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_active BOOLEAN DEFAULT TRUE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_vehicle (
    vehicle_id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    make VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL,
    production_year INT NOT NULL,
    license_plate VARCHAR(20) NOT NULL,
    vehicle_type_id INT NOT NULL,
    branch_id INT NOT NULL,
    rental_availability BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_active BOOLEAN DEFAULT TRUE,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    day_of_month INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week VARCHAR(15)
);

-- Fact Tables
CREATE TABLE fact_rental_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    vehicle_id INT REFERENCES dim_vehicle(vehicle_id),
    employee_id INT REFERENCES dim_employee(employee_id),
    rental_start_date_id INT REFERENCES dim_date(date_id),
    rental_end_date_id INT REFERENCES dim_date(date_id),
    payment_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    CONSTRAINT unique_transaction UNIQUE (customer_id, vehicle_id, employee_id, rental_start_date_id, rental_end_date_id)
);

CREATE TABLE fact_maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    vehicle_id INT REFERENCES dim_vehicle(vehicle_id),
    maintenance_date_id INT REFERENCES dim_date(date_id),
    service_type VARCHAR(100) NOT NULL,
    maintenance_cost DECIMAL(10, 2) NOT NULL,
    notes TEXT,
    CONSTRAINT unique_maintenance UNIQUE (vehicle_id, maintenance_date_id, service_type)
);

-- Create ETL control table to track the status of the ETL process
CREATE TABLE etl_control (
    etl_id SERIAL PRIMARY KEY,
    source_table VARCHAR(50),
    last_etl_time TIMESTAMP,
    status VARCHAR(20),
    records_processed INT,
    error_message TEXT
);

-- Dimension Tables Indexes
-- dim_customer
CREATE INDEX idx_customer_source_id ON dim_customer(source_id);
CREATE INDEX idx_customer_valid_dates ON dim_customer(valid_from, valid_to);
CREATE INDEX idx_customer_email ON dim_customer(email);

-- dim_branch
CREATE INDEX idx_branch_source_id ON dim_branch(source_id);
CREATE INDEX idx_branch_valid_dates ON dim_branch(valid_from, valid_to);

-- dim_employee
CREATE INDEX idx_employee_source_id ON dim_employee(source_id);
CREATE INDEX idx_employee_valid_dates ON dim_employee(valid_from, valid_to);
CREATE INDEX idx_employee_email ON dim_employee(email);
CREATE INDEX idx_employee_branch ON dim_employee(branch_id);

-- dim_vehicle_type
CREATE INDEX idx_vehicle_type_source_id ON dim_vehicle_type(source_id);
CREATE INDEX idx_vehicle_type_valid_dates ON dim_vehicle_type(valid_from, valid_to);

-- dim_vehicle
CREATE INDEX idx_vehicle_source_id ON dim_vehicle(source_id);
CREATE INDEX idx_vehicle_valid_dates ON dim_vehicle(valid_from, valid_to);
CREATE INDEX idx_vehicle_license_plate ON dim_vehicle(license_plate);
CREATE INDEX idx_vehicle_type ON dim_vehicle(vehicle_type_id);
CREATE INDEX idx_vehicle_branch ON dim_vehicle(branch_id);

-- dim_date
CREATE INDEX idx_date_full_date ON dim_date(full_date);
CREATE INDEX idx_date_year_month ON dim_date(year, month);

-- Fact Tables Indexes
-- fact_rental_transactions
CREATE INDEX idx_fact_rental_customer ON fact_rental_transactions(customer_id);
CREATE INDEX idx_fact_rental_vehicle ON fact_rental_transactions(vehicle_id);
CREATE INDEX idx_fact_rental_employee ON fact_rental_transactions(employee_id);
CREATE INDEX idx_fact_rental_dates ON fact_rental_transactions(rental_start_date_id, rental_end_date_id);

-- fact_maintenance
CREATE INDEX idx_fact_maintenance_vehicle ON fact_maintenance(vehicle_id);
CREATE INDEX idx_fact_maintenance_date ON fact_maintenance(maintenance_date_id);
