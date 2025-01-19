-- SQL Script to Create OLTP Database structure in 3NF
-- The database is named 'vehicle_rental' and is owned by the 'postgres' user.
-- =======================================================================================

-- Table: customer
CREATE TABLE customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone_number VARCHAR(15),
    address TEXT,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: branch
CREATE TABLE branch (
    branch_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(255) NOT NULL,
    contact_number VARCHAR(15)
);

-- Insert default values into branch table
INSERT INTO branch (name, location, contact_number) VALUES
('Downtown Branch', '123 Main St, Cityville', '1112223333'),
('Uptown Branch', '456 Market St, Cityville', '4445556666'),
('Midtown Branch', '789 Central Ave, Cityville', '7778889999'),
('Eastside Branch', '101 East St, Cityville', '2223334444'),
('Westside Branch', '202 West St, Cityville', '3334445555'),
('Southside Branch', '303 South St, Cityville', '4445556666'),
('Northside Branch', '404 North St, Cityville', '5556667777'),
('Central Branch', '505 Central Blvd, Cityville', '6667778888'),
('Airport Branch', '606 Airport Rd, Cityville', '7778889999'),
('Harbor Branch', '707 Harbor Dr, Cityville', '8889990000'),
('Suburban Branch', '808 Suburban Ln, Cityville', '9990001111'),
('Industrial Branch', '909 Industrial Rd, Cityville', '0001112222'),
('University Branch', '1010 College Ave, Cityville', '1112223333');

-- Table: Employee
CREATE TABLE employee (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone_number VARCHAR(15),
    branch_id INT NOT NULL,
    FOREIGN KEY (branch_id) REFERENCES branch(branch_id)
);

INSERT INTO employee (first_name, last_name, email, phone_number, branch_id) VALUES
('John', 'Smith', 'john.smith1@branch1.com', '555-1001', 1),
('Emily', 'Johnson', 'emily.johnson1@branch1.com', '555-1002', 1),
('Michael', 'Brown', 'michael.brown2@branch2.com', '555-2001', 2),
('Sarah', 'Davis', 'sarah.davis2@branch2.com', '555-2002', 2),
('Robert', 'Wilson', 'robert.wilson3@branch3.com', '555-3001', 3),
('Linda', 'Taylor', 'linda.taylor3@branch3.com', '555-3002', 3),
('James', 'Martinez', 'james.martinez4@branch4.com', '555-4001', 4),
('Laura', 'White', 'laura.white4@branch4.com', '555-4002', 4),
('Richard', 'Clark', 'richard.clark5@branch5.com', '555-5001', 5),
('Nancy', 'Lewis', 'nancy.lewis5@branch5.com', '555-5002', 5),
('Christopher', 'Walker', 'christopher.walker6@branch6.com', '555-6001', 6),
('Mary', 'Hall', 'mary.hall6@branch6.com', '555-6002', 6),
('Joseph', 'Allen', 'joseph.allen7@branch7.com', '555-7001', 7),
('Barbara', 'Young', 'barbara.young7@branch7.com', '555-7002', 7),
('Charles', 'King', 'charles.king8@branch8.com', '555-8001', 8),
('Susan', 'Wright', 'susan.wright8@branch8.com', '555-8002', 8),
('David', 'Scott', 'david.scott9@branch9.com', '555-9001', 9),
('Patricia', 'Adams', 'patricia.adams9@branch9.com', '555-9002', 9),
('Daniel', 'Baker', 'daniel.baker10@branch10.com', '555-10001', 10),
('Lisa', 'Gonzalez', 'lisa.gonzalez10@branch10.com', '555-10002', 10),
('Mark', 'Perez', 'mark.perez11@branch11.com', '555-11001', 11),
('Karen', 'Lopez', 'karen.lopez11@branch11.com', '555-11002', 11),
('Steven', 'Hill', 'steven.hill12@branch12.com', '555-12001', 12),
('Sandra', 'Edwards', 'sandra.edwards12@branch12.com', '555-12002', 12),
('Paul', 'Green', 'paul.green13@branch13.com', '555-13001', 13),
('Donna', 'Brooks', 'donna.brooks13@branch13.com', '555-13002', 13);


-- Table: vehicle_type
CREATE TABLE vehicle_type (
    vehicle_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL,
    description TEXT
);

INSERT INTO vehicle_type (type_name, description) VALUES
('Sedan', 'Comfortable 4-door cars suitable for families'),
('SUV', 'Spacious vehicles for off-road and family trips'),
('Truck', 'Large vehicles for cargo transport'),
('Van', 'Multi-purpose vehicles suitable for group travels'),
('Motorcycle', 'Two-wheeled motorized vehicles'),
('Convertible', 'Cars with retractable roofs'),
('Coupe', 'Compact and stylish two-door cars designed for individual or couple use'),
('Heavy Duty Truck', 'Trucks designed for heavy cargo transport'),
('Camper', 'Recreational vehicles for camping'),
('Electric Car', 'Vehicles powered entirely by electricity'),
('Hybrid Car', 'Vehicles powered by a combination of gasoline and electricity'),
('Sports Car', 'High-performance and stylish cars'),
('Compact Car', 'Small size and compact vehicles for city roads'),
('Minivan', 'Family-oriented vehicles with extra seating'),
('Pickup Truck', 'Light-duty trucks with open cargo areas'),
('Luxury SUV', 'High-end spacious vehicles for off-road trips with premium features'),
('Luxury Sedan', 'High-end sedans with premium features');

-- Table: Vehicle
CREATE TABLE vehicle (
    vehicle_id SERIAL PRIMARY KEY,
    make VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL,
    production_year INT NOT NULL,
    license_plate VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type_id INT NOT NULL,
    branch_id INT NOT NULL,
    rental_availability BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (vehicle_type_id) REFERENCES vehicle_type(vehicle_type_id),
    FOREIGN KEY (branch_id) REFERENCES branch(branch_id),
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: rental_transaction
CREATE TABLE rental_transaction (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    vehicle_id INT NOT NULL,
    employee_id INT NOT NULL,
    rental_start_date DATE NOT NULL,
    rental_end_date DATE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id),
    FOREIGN KEY (employee_id) REFERENCES employee(employee_id),
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: Payment
CREATE TABLE payment (
    payment_id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL,
    payment_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES rental_transaction(transaction_id)
);


-- Create Rental Transaction View to dynamically calculate the total rental cost based on payments
CREATE OR REPLACE VIEW rental_transaction_view AS
SELECT 
    rt.transaction_id,
    rt.customer_id,
    rt.vehicle_id,
    rt.employee_id,
    rt.rental_start_date,
    rt.rental_end_date,
    COALESCE(SUM(p.amount), 0) AS total_cost -- Sum of payments
FROM 
    rental_transaction rt
LEFT JOIN 
    payment p ON rt.transaction_id = p.transaction_id
GROUP BY 
    rt.transaction_id, rt.customer_id, rt.vehicle_id, rt.employee_id, rt.rental_start_date, rt.rental_end_date;

-- Table: Maintenance
CREATE TABLE maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    vehicle_id INT NOT NULL,
    service_date DATE NOT NULL,
    service_type VARCHAR(100) NOT NULL,
    maintenance_cost DECIMAL(10, 2) NOT NULL,
    notes TEXT,
    FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id)
);

-- Create triggers to update last_modified
CREATE OR REPLACE FUNCTION update_last_modified()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_last_modified
    BEFORE UPDATE ON customer
    FOR EACH ROW
    EXECUTE FUNCTION update_last_modified();

-- (Similar triggers for vehicle and rental_transaction tables)
CREATE TRIGGER vehicle_last_modified
    BEFORE UPDATE ON vehicle
    FOR EACH ROW
    EXECUTE FUNCTION update_last_modified();

CREATE TRIGGER rental_transaction_last_modified
    BEFORE UPDATE ON rental_transaction
    FOR EACH ROW
    EXECUTE FUNCTION update_last_modified();
