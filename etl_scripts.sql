-- This script contains functions for ETL operations between the OLTP and DWH databases.
-- It includes functions for populating the date dimension, updating dimensions, and updating facts.
-- =======================================================================================

-- In the DWH (OLAP) database
-- Create extension if not exists
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create server connection to OLTP
CREATE SERVER oltp_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', port '5432', dbname 'vehicle_rental');

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER oltp_server
OPTIONS (user 'postgres', password 'postgres');

-- Create foreign schema
CREATE SCHEMA oltp;
IMPORT FOREIGN SCHEMA public 
FROM SERVER oltp_server 
INTO oltp;

-- Function to populate/update dim_date
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS void AS $$
BEGIN
    INSERT INTO dim_date (
        full_date,
        day_of_month,
        month,
        year,
        quarter,
        day_of_week
    )
    SELECT
        d::DATE as full_date,
        EXTRACT(DAY FROM d) as day_of_month,
        EXTRACT(MONTH FROM d) as month,
        EXTRACT(YEAR FROM d) as year,
        EXTRACT(QUARTER FROM d) as quarter,
        TO_CHAR(d, 'Day') as day_of_week
    FROM generate_series(start_date, end_date, '1 day'::interval) d
    ON CONFLICT (full_date) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Function to update dim_customer (SCD Type 2)
CREATE OR REPLACE FUNCTION update_dim_customer()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'customer';

    -- Insert new records and update changed ones
    WITH new_customers AS (
        SELECT 
            c.customer_id as source_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone_number,
            c.address,
            CURRENT_TIMESTAMP as valid_from,
            '9999-12-31'::TIMESTAMP as valid_to,
            TRUE as is_active
        FROM oltp.customer c
        WHERE c.last_modified > v_last_etl_time
    )
    MERGE INTO dim_customer t
    USING new_customers s
    ON t.source_id = s.source_id AND t.valid_to = '9999-12-31'::TIMESTAMP
    WHEN MATCHED AND (
        t.first_name != s.first_name OR
        t.last_name != s.last_name OR
        t.email != s.email OR
        t.phone_number IS DISTINCT FROM s.phone_number OR
        t.address IS DISTINCT FROM s.address
    ) THEN
        UPDATE SET valid_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
                   is_active = FALSE
    WHEN NOT MATCHED THEN
        INSERT (source_id, first_name, last_name, email, phone_number, address, 
                valid_from, valid_to, is_active)
        VALUES (s.source_id, s.first_name, s.last_name, s.email, s.phone_number, 
                s.address, s.valid_from, s.valid_to, s.is_active);

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('customer', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Function to update dim_vehicle (SCD Type 2)
CREATE OR REPLACE FUNCTION update_dim_vehicle()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'vehicle';

    -- Insert new records and update changed ones
    WITH new_vehicles AS (
        SELECT 
            v.vehicle_id as source_id,
            v.make,
            v.model,
            v.production_year,
            v.license_plate,
            v.vehicle_type_id,
            v.branch_id,
            v.rental_availability,
            CURRENT_TIMESTAMP as valid_from,
            '9999-12-31'::TIMESTAMP as valid_to,
            TRUE as is_active
        FROM oltp.vehicle v
        WHERE v.last_modified > v_last_etl_time
    )
    MERGE INTO dim_vehicle t
    USING new_vehicles s
    ON t.source_id = s.source_id AND t.valid_to = '9999-12-31'::TIMESTAMP
    WHEN MATCHED AND (
        t.make != s.make OR
        t.model != s.model OR
        t.production_year != s.production_year OR
        t.license_plate != s.license_plate OR
        t.vehicle_type_id != s.vehicle_type_id OR
        t.branch_id != s.branch_id OR
        t.rental_availability != s.rental_availability
    ) THEN
        UPDATE SET valid_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
                   is_active = FALSE
    WHEN NOT MATCHED THEN
        INSERT (source_id, make, model, production_year, license_plate, 
                vehicle_type_id, branch_id, rental_availability,
                valid_from, valid_to, is_active)
        VALUES (s.source_id, s.make, s.model, s.production_year, s.license_plate,
                s.vehicle_type_id, s.branch_id, s.rental_availability,
                s.valid_from, s.valid_to, s.is_active);

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('vehicle', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Function to update dim_employee (SCD Type 2)
CREATE OR REPLACE FUNCTION update_dim_employee()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'employee';

    -- Insert new records and update changed ones
    WITH new_employees AS (
        SELECT 
            e.employee_id as source_id,
            e.first_name,
            e.last_name,
            e.email,
            e.phone_number,
            e.branch_id,
            CURRENT_TIMESTAMP as valid_from,
            '9999-12-31'::TIMESTAMP as valid_to,
            TRUE as is_active
        FROM oltp.employee e
    )
    MERGE INTO dim_employee t
    USING new_employees s
    ON t.source_id = s.source_id AND t.valid_to = '9999-12-31'::TIMESTAMP
    WHEN MATCHED AND (
        t.first_name != s.first_name OR
        t.last_name != s.last_name OR
        t.email != s.email OR
        t.phone_number IS DISTINCT FROM s.phone_number OR
        t.branch_id != s.branch_id
    ) THEN
        UPDATE SET valid_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
                   is_active = FALSE
    WHEN NOT MATCHED THEN
        INSERT (source_id, first_name, last_name, email, phone_number, branch_id,
                valid_from, valid_to, is_active)
        VALUES (s.source_id, s.first_name, s.last_name, s.email, s.phone_number,
                s.branch_id, s.valid_from, s.valid_to, s.is_active);

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('employee', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Function to update fact_rental_transactions
CREATE OR REPLACE FUNCTION update_fact_rental_transactions()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'rental_transaction';

    -- Insert transactions
    INSERT INTO fact_rental_transactions (
        customer_id,
        vehicle_id,
        employee_id,
        rental_start_date_id,
        rental_end_date_id,
        payment_amount,
        payment_method
    )
    WITH rental_payments AS (
        SELECT 
            rt.transaction_id,
            rt.customer_id as source_customer_id,
            rt.vehicle_id as source_vehicle_id,
            rt.employee_id as source_employee_id,
            rt.rental_start_date,
            rt.rental_end_date,
            LEFT(STRING_AGG(DISTINCT p.payment_method, ', ' ORDER BY p.payment_method), 50) as payment_method,
            SUM(p.amount) as total_amount
        FROM oltp.rental_transaction rt
        LEFT JOIN oltp.payment p ON p.transaction_id = rt.transaction_id
        GROUP BY rt.transaction_id, rt.customer_id, rt.vehicle_id, 
                 rt.employee_id, rt.rental_start_date, rt.rental_end_date
    )
    SELECT 
        dc.customer_id,
        dv.vehicle_id,
        de.employee_id,
        dd_start.date_id as rental_start_date_id,
        dd_end.date_id as rental_end_date_id,
        rp.total_amount as payment_amount,
        rp.payment_method
    FROM rental_payments rp
    JOIN dim_customer dc ON rp.source_customer_id = dc.source_id 
        AND CURRENT_TIMESTAMP BETWEEN dc.valid_from AND dc.valid_to
    JOIN dim_vehicle dv ON rp.source_vehicle_id = dv.source_id 
        AND CURRENT_TIMESTAMP BETWEEN dv.valid_from AND dv.valid_to
    JOIN dim_employee de ON rp.source_employee_id = de.source_id
        AND CURRENT_TIMESTAMP BETWEEN de.valid_from AND de.valid_to
    JOIN dim_date dd_start ON rp.rental_start_date::date = dd_start.full_date
    JOIN dim_date dd_end ON rp.rental_end_date::date = dd_end.full_date
    ON CONFLICT (customer_id, vehicle_id, employee_id, rental_start_date_id, rental_end_date_id) DO NOTHING;

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('rental_transaction', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Function to update fact_maintenance
CREATE OR REPLACE FUNCTION update_fact_maintenance()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'maintenance';

    -- Insert new maintenance records
    INSERT INTO fact_maintenance (
        vehicle_id,
        maintenance_date_id,
        service_type,
        maintenance_cost,
        notes
    )
    SELECT 
        dv.vehicle_id,
        dd.date_id as maintenance_date_id,
        m.service_type,
        m.maintenance_cost,
        m.notes
    FROM oltp.maintenance m
    JOIN dim_vehicle dv ON m.vehicle_id = dv.source_id 
        AND CURRENT_TIMESTAMP BETWEEN dv.valid_from AND dv.valid_to
    JOIN dim_date dd ON m.service_date = dd.full_date
    ON CONFLICT (vehicle_id, maintenance_date_id, service_type) DO NOTHING;

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('maintenance', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Function to update dim_branch (SCD Type 2)
CREATE OR REPLACE FUNCTION update_dim_branch()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'branch';

    -- Insert new records and update changed ones
    WITH new_branches AS (
        SELECT 
            b.branch_id as source_id,
            b.name,
            b.location,
            b.contact_number,
            CURRENT_TIMESTAMP as valid_from,
            '9999-12-31'::TIMESTAMP as valid_to,
            TRUE as is_active
        FROM oltp.branch b
    )
    MERGE INTO dim_branch t
    USING new_branches s
    ON t.source_id = s.source_id AND t.valid_to = '9999-12-31'::TIMESTAMP
    WHEN MATCHED AND (
        t.name != s.name OR
        t.location != s.location OR
        t.contact_number IS DISTINCT FROM s.contact_number
    ) THEN
        UPDATE SET valid_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
                   is_active = FALSE
    WHEN NOT MATCHED THEN
        INSERT (source_id, name, location, contact_number, valid_from, valid_to, is_active)
        VALUES (s.source_id, s.name, s.location, s.contact_number, 
                s.valid_from, s.valid_to, s.is_active);

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('branch', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Function to update dim_vehicle_type (SCD Type 2)
CREATE OR REPLACE FUNCTION update_dim_vehicle_type()
RETURNS void AS $$
DECLARE
    v_last_etl_time TIMESTAMP;
    v_count INT;
BEGIN
    -- Get last ETL time
    SELECT COALESCE(MAX(last_etl_time), '1900-01-01'::TIMESTAMP)
    INTO v_last_etl_time
    FROM etl_control
    WHERE source_table = 'vehicle_type';

    -- Insert new records and update changed ones
    WITH new_vehicle_types AS (
        SELECT 
            vt.vehicle_type_id as source_id,
            vt.type_name,
            vt.description,
            CURRENT_TIMESTAMP as valid_from,
            '9999-12-31'::TIMESTAMP as valid_to,
            TRUE as is_active
        FROM oltp.vehicle_type vt
    )
    MERGE INTO dim_vehicle_type t
    USING new_vehicle_types s
    ON t.source_id = s.source_id AND t.valid_to = '9999-12-31'::TIMESTAMP
    WHEN MATCHED AND (
        t.type_name != s.type_name OR
        t.description IS DISTINCT FROM s.description
    ) THEN
        UPDATE SET valid_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
                   is_active = FALSE
    WHEN NOT MATCHED THEN
        INSERT (source_id, type_name, description, valid_from, valid_to, is_active)
        VALUES (s.source_id, s.type_name, s.description, s.valid_from, s.valid_to, s.is_active);

    -- Get count of processed records
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Update ETL control
    INSERT INTO etl_control (source_table, last_etl_time, status, records_processed)
    VALUES ('vehicle_type', CURRENT_TIMESTAMP, 'SUCCESS', v_count);
END;
$$ LANGUAGE plpgsql;

-- Main ETL function
CREATE OR REPLACE FUNCTION run_etl()
RETURNS void AS $$
BEGIN
    -- Populate date dimension from 2023 to 5 years in the future
    PERFORM populate_dim_date(
        '2023-01-01'::DATE,  -- Start from 2023
        (CURRENT_DATE + INTERVAL '5 years')::DATE
    );
    
    -- Update dimensions
    PERFORM update_dim_vehicle_type();
    PERFORM update_dim_branch();
    PERFORM update_dim_customer();
    PERFORM update_dim_vehicle();
    PERFORM update_dim_employee();
    
    -- Update facts
    PERFORM update_fact_rental_transactions();
    PERFORM update_fact_maintenance();
    
EXCEPTION WHEN OTHERS THEN
    -- Log error and rollback
    INSERT INTO etl_control (source_table, last_etl_time, status, error_message)
    VALUES ('ETL_FULL', CURRENT_TIMESTAMP, 'ERROR', SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql; 
