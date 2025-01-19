-- OLTP Query 1: "What are the top 5 most rented vehicles (by number of rentals) and their total revenue?"
SELECT 
    v.make as "Make",
    v.model as "Model",
    v.license_plate as "License Plate",
    COUNT(rt.transaction_id) as "Rental Count",
    SUM(rt.total_cost) as "Total Revenue ($)"
FROM oltp.vehicle v
JOIN oltp.rental_transaction_view rt ON v.vehicle_id = rt.vehicle_id
GROUP BY v.vehicle_id, v.make, v.model, v.license_plate
ORDER BY "Rental Count" DESC
LIMIT 5;

-- OLTP Query 2: "What is the average rental duration and revenue per vehicle type?"
SELECT 
    vt.type_name as "Vehicle Type",
    ROUND(AVG(EXTRACT(DAY FROM (rt.rental_end_date::timestamp - rt.rental_start_date::timestamp))), 1) as "Average Rental Duration (Days)",
    ROUND(AVG(rt.total_cost), 2) as "Average Rental Revenue ($)"
FROM oltp.vehicle_type vt
JOIN oltp.vehicle v ON vt.vehicle_type_id = v.vehicle_type_id
JOIN oltp.rental_transaction_view rt ON v.vehicle_id = rt.vehicle_id
GROUP BY vt.type_name
ORDER BY "Average Rental Revenue ($)" DESC;

-- OLAP Query 1: "What are the top 5 most profitable vehicles including their maintenance costs?"
SELECT 
    dv.make as "Make",
    dv.model as "Model",
    dv.license_plate as "License Plate",
    COALESCE(SUM(fr.payment_amount), 0) as "Total Revenue ($)",
    COALESCE(SUM(fm.maintenance_cost), 0) as "Total Maintenance Cost ($)",
    COALESCE(SUM(fr.payment_amount), 0) - COALESCE(SUM(fm.maintenance_cost), 0) as "Net Profit ($)"
FROM dim_vehicle dv
JOIN fact_rental_transactions fr ON dv.vehicle_id = fr.vehicle_id
LEFT JOIN fact_maintenance fm ON dv.vehicle_id = fm.vehicle_id
GROUP BY dv.vehicle_id, dv.make, dv.model, dv.license_plate
HAVING COALESCE(SUM(fr.payment_amount), 0) > 0
ORDER BY "Net Profit ($)" DESC
LIMIT 5;

-- OLAP Query 2: "What is the monthly revenue trend by vehicle type?"
SELECT 
    dd.year as "Year",
    TO_CHAR(TO_DATE(dd.month::text, 'MM'), 'Month') as "Month",
    dvt.type_name as "Vehicle Type",
    SUM(fr.payment_amount) as "Monthly Revenue ($)",
    COUNT(fr.transaction_id) as "Rental Count"
FROM fact_rental_transactions fr
JOIN dim_vehicle dv ON fr.vehicle_id = dv.vehicle_id
JOIN dim_vehicle_type dvt ON dv.vehicle_type_id = dvt.vehicle_type_id
JOIN dim_date dd ON fr.rental_start_date_id = dd.date_id
GROUP BY dd.year, dd.month, dvt.type_name
ORDER BY dd.year, dd.month, "Monthly Revenue ($)" DESC;
