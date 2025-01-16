-- Loading vehicle data from a csv file into OLTP database ('vehicle_rental')
-- =======================================================================================

CREATE OR REPLACE FUNCTION load_vehicle_data(file_path TEXT)
RETURNS VOID AS $$
DECLARE
    record RECORD; 
    v_type_id INT;
    brch_id INT;
BEGIN
    -- Step 1: Create a temporary staging table
    CREATE TEMP TABLE vehicle_staging (
        make VARCHAR(50),
        model VARCHAR(50),
        production_year INT,
        license_plate VARCHAR(20),
        vehicle_type_name VARCHAR(50),
        branch_name VARCHAR(100),
        rental_status_text VARCHAR(20)
    );

    -- Step 2: Load data into staging table using COPY
    EXECUTE FORMAT('COPY vehicle_staging FROM %L WITH CSV HEADER', file_path);

    -- Step 3: Process records in the staging table
    FOR record IN SELECT * FROM vehicle_staging LOOP
        -- Validate and fetch VehicleTypeID
        SELECT vehicle_type_id INTO v_type_id
        FROM vehicle_type
        WHERE type_name = record.vehicle_type_name;

        IF v_type_id IS NULL THEN
            RAISE NOTICE 'Skipping record: Vehicle type % not found', record.vehicle_type_name;
            CONTINUE;
        END IF;

        -- Validate and fetch BranchID
        SELECT branch_id INTO brch_id
        FROM branch
        WHERE name = record.branch_name;

        IF brch_id IS NULL THEN
            RAISE NOTICE 'Skipping record: Branch % not found', record.branch_name;
            CONTINUE;
        END IF;

        -- Insert validated data
        BEGIN
            INSERT INTO vehicle (make, model, production_year, license_plate, vehicle_type_id, branch_id, rental_availability)
            VALUES (
                record.make,
                record.model,
                record.production_year,
                record.license_plate,
                v_type_id,
                brch_id,
                CASE LOWER(record.rental_status_text)
                    WHEN 'available' THEN TRUE
                    ELSE FALSE
                END
            );
        EXCEPTION WHEN unique_violation THEN
            RAISE NOTICE 'Vehicle with license plate: % is already in the database. Skipping...', record.license_plate;
        END;
    END LOOP;

    -- Step 4: Cleanup temporary staging table
    DROP TABLE vehicle_staging;

    RAISE NOTICE 'Data loading completed successfully';
END;
$$ LANGUAGE plpgsql;

-- =======================================================================================
-- Load customer data from a csv file into OLTP database ('vehicle_rental')
CREATE OR REPLACE FUNCTION load_customer_data(file_path TEXT)
RETURNS VOID AS $$
DECLARE
    record RECORD;
BEGIN
    -- Step 1: Create a temporary staging table
    CREATE TEMP TABLE customer_staging (
        first_name VARCHAR(50),
        last_name VARCHAR(50),
		email VARCHAR(100),
		phone_number VARCHAR(15),
		address TEXT
    );
	
	-- Step 2: Load data into staging table using COPY
    EXECUTE FORMAT('COPY customer_staging FROM %L WITH CSV HEADER', file_path);
	
	-- Step 3: Process records in the staging table
    FOR record IN SELECT * FROM customer_staging LOOP
		-- Insert data
        BEGIN
            INSERT INTO customer (first_name, last_name, email, phone_number, address)
            VALUES (
                record.first_name,
                record.last_name,
                record.email,
                record.phone_number,
                record.address
            );
        EXCEPTION WHEN unique_violation THEN
            RAISE NOTICE 'Customer with email: % is already in the database. Skipping...', record.email;
        END;
    END LOOP;

    -- Step 4: Cleanup temporary staging table
    DROP TABLE customer_staging;

    RAISE NOTICE 'Data loading completed successfully';
END;
$$ LANGUAGE plpgsql;

-- =======================================================================================
-- Load transactions data to OLTP database ('vehicle_rental')

-- Step 1: Function to load transactions
CREATE OR REPLACE FUNCTION load_transactions_from_csv(file_path TEXT)
RETURNS VOID AS $$
DECLARE
    v_customer_id INT;
    v_vehicle_id INT;
    v_employee_id INT;
    record RECORD;
	existing_transaction_count INT;
BEGIN
	-- Step 2: Create a staging table
	CREATE TEMP TABLE transaction_staging (
		customer_first_name VARCHAR(50),
		customer_last_name VARCHAR(50),
		vehicle_make VARCHAR(50),
		vehicle_model VARCHAR(50),
		vehicle_license_plate VARCHAR(20),
		employee_first_name VARCHAR(50),
		employee_last_name VARCHAR(50),
		rental_start_date DATE,
		rental_end_date DATE
	);
    -- Load data from CSV into staging table
    EXECUTE FORMAT('COPY transaction_staging FROM %L WITH CSV HEADER', file_path);

    -- Process each row in the staging table
    FOR record IN SELECT * FROM transaction_staging LOOP
        -- Fetch customer_id
        SELECT customer_id
        INTO v_customer_id
        FROM customer
        WHERE first_name = record.customer_first_name
          AND last_name = record.customer_last_name;

        IF v_customer_id IS NULL THEN
            RAISE NOTICE 'Customer not found: %, %', record.customer_first_name, record.customer_last_name;
            CONTINUE;
        END IF;

        -- Fetch vehicle_id
        SELECT vehicle_id
        INTO v_vehicle_id
        FROM vehicle
        WHERE license_plate = record.vehicle_license_plate;

        IF v_vehicle_id IS NULL THEN
            RAISE NOTICE 'Vehicle not found: %', record.vehicle_license_plate;
            CONTINUE;
        END IF;

        -- Fetch employee_id
        SELECT employee_id
        INTO v_employee_id
        FROM employee
        WHERE first_name = record.employee_first_name
          AND last_name = record.employee_last_name;

        IF v_employee_id IS NULL THEN
            RAISE NOTICE 'Employee not found: %, %', record.employee_first_name, record.employee_last_name;
            CONTINUE;
        END IF;

        -- Check for existing transaction with same vehicle and rental dates
        SELECT COUNT(*)
        INTO existing_transaction_count
        FROM rental_transaction
        WHERE vehicle_id = v_vehicle_id
          AND rental_start_date = record.rental_start_date
          AND rental_end_date = record.rental_end_date;

        IF existing_transaction_count > 0 THEN
            RAISE NOTICE 'Duplicate transaction found for Vehicle % with Rental Dates % to %. Skipping.',
                record.vehicle_license_plate, record.rental_start_date, record.rental_end_date;
            CONTINUE;
        END IF;

        -- Insert valid row into rental_transaction
        INSERT INTO rental_transaction (
            customer_id, vehicle_id, employee_id, rental_start_date, rental_end_date
        ) VALUES (
            v_customer_id, v_vehicle_id, v_employee_id, record.rental_start_date, record.rental_end_date
        );

        RAISE NOTICE 'Transaction added successfully: Customer %, Vehicle %, Employee %',
            v_customer_id, v_vehicle_id, v_employee_id;
    END LOOP;

    -- Clear staging table after processing
    DROP TABLE transaction_staging;
	
	RAISE NOTICE 'Data loading completed successfully';
END;
$$ LANGUAGE plpgsql;

-- =======================================================================================
-- Load payments data into OLTP database ('vehicle_rental')
-- Step 1: Function to load transactions
CREATE OR REPLACE FUNCTION load_payments_from_csv(file_path TEXT)
RETURNS VOID AS $$
DECLARE
    temp_transaction_id INT;
	temp_vehicle_id INT;
    record RECORD;
BEGIN
	-- Step 2: Create a staging table
	CREATE TEMP TABLE payment_staging (
		customer_name VARCHAR(100),
		vehicle_license_plate VARCHAR(20),
		rental_start_date DATE,
		payment_date DATE,
		amount NUMERIC(10, 2),
		payment_method VARCHAR(100)
	);
	-- Load data from CSV into staging table
    EXECUTE FORMAT('COPY payment_staging FROM %L WITH CSV HEADER', file_path);

	-- Process each row in the staging table
    FOR record IN SELECT * FROM payment_staging LOOP
		-- Fetch vehicle_id
        SELECT vehicle_id
        INTO temp_vehicle_id
        FROM vehicle
        WHERE license_plate = record.vehicle_license_plate;
		
		IF temp_vehicle_id IS NULL THEN
			RAISE NOTICE 'Vehicle not found: %', record.vehicle_license_plate;
			CONTINUE;
		END IF;
	
		-- Fetch transaction_id
		SELECT transaction_id
		INTO temp_transaction_id
		FROM rental_transaction
		WHERE vehicle_id = temp_vehicle_id
			AND rental_start_date = record.rental_start_date;
		
		IF temp_transaction_id IS NULL THEN
			RAISE NOTICE 'The transaction for car % with rental date % does not exist', record.vehicle_license_plate, record.rental_start_date;
			CONTINUE;
		END IF;
		
		-- Insert valid row into payment
        INSERT INTO payment (
            transaction_id, payment_date, amount, payment_method
        ) VALUES (
            temp_transaction_id, record.payment_date, record.amount, record.payment_method
        );

        RAISE NOTICE 'Payment added successfully: Payment date %, Vehicle %, Amount %',
            record.payment_date, record.vehicle_license_plate, record.amount;
    END LOOP;
	
	-- Clear staging table after processing
    DROP TABLE payment_staging;
	
	RAISE NOTICE 'Payment data loading completed successfully';
END;
$$ LANGUAGE plpgsql;

-- =======================================================================================
-- Load vehicle maintenance data from csv file into OLTP database ('vehicle_rental')
-- Step 1: Function to load maintenance data
CREATE OR REPLACE FUNCTION load_maintenance_from_csv(file_path TEXT)
RETURNS VOID AS $$
DECLARE
	temp_vehicle_id INT;
	existing_record_count INT;
    record RECORD;
BEGIN
	-- Step 2: Create a staging table
	CREATE TEMP TABLE maintenance_staging (
		vehicle_license_plate VARCHAR(20),
		service_date DATE,
		service_type VARCHAR(100),
		maintenance_cost NUMERIC(10, 2),
		notes TEXT
	);
	-- Load data from CSV into staging table
    EXECUTE FORMAT('COPY maintenance_staging FROM %L WITH CSV HEADER', file_path);

	-- Process each row in the staging table
    FOR record IN SELECT * FROM maintenance_staging LOOP
		-- Fetch vehicle_id
        SELECT vehicle_id
        INTO temp_vehicle_id
        FROM vehicle
        WHERE license_plate = record.vehicle_license_plate;
		
		IF temp_vehicle_id IS NULL THEN
			RAISE NOTICE 'Vehicle not found: %', record.vehicle_license_plate;
			CONTINUE;
		END IF;
		
		-- Check for existing transaction with same vehicle and rental dates
        SELECT COUNT(*)
        INTO existing_record_count
        FROM maintenance
        WHERE vehicle_id = temp_vehicle_id
          AND service_date = record.service_date
          AND service_type = record.service_type;

        IF existing_record_count > 0 THEN
            RAISE NOTICE 'Duplicate maintenance record found for Vehicle % with Service Date: %. Skipping.',
                record.vehicle_license_plate, record.service_date;
            CONTINUE;
        END IF;
		
		-- Insert valid row into maintenance
		INSERT INTO maintenance (
			vehicle_id, service_date, service_type, maintenance_cost, notes
		) VALUES (
			temp_vehicle_id, record.service_date, record.service_type, record.maintenance_cost, record.notes
		);
		
		RAISE NOTICE 'Maintenance record added successfully: Service date %, Vehicle %, Amount %',
            record.service_date, record.vehicle_license_plate, record.maintenance_cost;
    END LOOP;
	
	-- Clear staging table after processing
    DROP TABLE maintenance_staging;
	
	RAISE NOTICE 'maintenance data loading completed successfully';
END;
$$ LANGUAGE plpgsql;
