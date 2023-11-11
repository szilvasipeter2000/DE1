-- ------------------------------------------------------------------------
-- ----- Creating a Denormalized data structure as my data warehouse ------
-- ------------------------------------------------------------------------
USE term1;
DROP PROCEDURE IF EXISTS CreateAnalyticalLayer;

DELIMITER //

CREATE PROCEDURE CreateAnalyticalLayer()
BEGIN

	DROP TABLE IF EXISTS analytical_layer;
    
	CREATE TABLE analytical_layer AS
		SELECT
			o.orderID,
			o.customerID,
			o.employeeID,
			o.orderDate,
			o.requiredDate,
			o.shippedDate,
			o.shipperID,
			o.freight,
			od.productID,
			od.unitPrice AS unitPrice_order_details,
			od.quantity AS quantity_order_details,
            (od.unitPrice * od.quantity) AS Revenue,
			od.discount,
			c.companyName AS customer_companyName,
			c.contactName AS customer_contactName,
			c.contactTitle AS customer_contactTitle,
			c.city AS customer_city,
			c.country AS customer_country,
			p.productName,
			p.quantityPerUnit,
			p.unitPrice AS unitPrice_products,
			p.discontinued AS product_discontinued,
			p.categoryID AS product_categoryID,
			cat.categoryName AS category_name,
			cat.description AS category_description,
			e.employeeName,
			e.title AS employee_title,
			e.city AS employee_city,
			e.country AS employee_country,
			e.reportsTo AS employee_reportsTo,
			s.companyName AS shipper_companyName
		FROM
			orders o
			INNER JOIN order_details od ON o.orderID = od.orderID
			INNER JOIN customers c ON o.customerID = c.customerID
			INNER JOIN products p ON od.productID = p.productID
			INNER JOIN categories cat ON p.categoryID = cat.categoryID
			INNER JOIN employees e ON o.employeeID = e.employeeID
			INNER JOIN shippers s ON o.shipperID = s.shipperID;
    END//
    
    DELIMITER ;
    CALL CreateAnalyticalLayer;
    
-- ----------------------------------------
-- -------- Creating an Event -------------				
-- ----------------------------------------
SET GLOBAL event_scheduler = ON;
-- Create an event that updates the analytical_layer at the beginning of every month.
DROP EVENT IF EXISTS monthly_analytical_layer;

DELIMITER //

CREATE EVENT IF NOT EXISTS monthly_analytical_layer
ON SCHEDULE EVERY 1 MONTH
STARTS LAST_DAY(CURRENT_DATE) + INTERVAL 1 DAY
DO
BEGIN
    CALL CreateAnalyticalLayer;
END //

DELIMITER ;

-- -----------------------------------------
-- -------- Creating a Trigger -------------				
-- -----------------------------------------
DROP TRIGGER IF EXISTS after_order_insert; 
-- each new order is logged here:
CREATE TABLE log_book (logged varchar(255) NOT NULL);
DELIMITER $$

CREATE TRIGGER after_order_insert
AFTER INSERT
ON order_details FOR EACH ROW
BEGIN
	
	-- log the order number of the newley inserted order
    	INSERT INTO log_book SELECT CONCAT('new orderID: ', NEW.orderID);

	-- archive the order and assosiated table entries to analytical_layer
  	INSERT INTO analytical_layer
	SELECT
			o.orderID,
			o.customerID,
			o.employeeID,
			o.orderDate,
			o.requiredDate,
			o.shippedDate,
			o.shipperID,
			o.freight,
			od.productID,
			od.unitPrice AS unitPrice_order_details,
			od.quantity AS quantity_order_details,
			od.discount,
			c.companyName AS customer_companyName,
			c.contactName AS customer_contactName,
			c.contactTitle AS customer_contactTitle,
			c.city AS customer_city,
			c.country AS customer_country,
			p.productName,
			p.quantityPerUnit,
			p.unitPrice AS unitPrice_products,
			p.discontinued AS product_discontinued,
			p.categoryID AS product_categoryID,
			cat.categoryName AS category_name,
			cat.description AS category_description,
			e.employeeName,
			e.title AS employee_title,
			e.city AS employee_city,
			e.country AS employee_country,
			e.reportsTo AS employee_reportsTo,
			s.companyName AS shipper_companyName
		FROM
			orders o
			LEFT JOIN order_details od ON o.orderID = od.orderID
			LEFT JOIN customers c ON o.customerID = c.customerID
			LEFT JOIN products p ON od.productID = p.productID
			LEFT JOIN categories cat ON p.categoryID = cat.categoryID
			LEFT JOIN employees e ON o.employeeID = e.employeeID
			LEFT JOIN shippers s ON o.shipperID = s.shipperID
	WHERE o.orderID = NEW.orderID;        
END $$

DELIMITER ;

-- Testing:
-- Check analytical_layer first:
-- SELECT * FROM analytical_layer Order by orderDate DESC;
-- 2155 rows returned and last orderDate 2015-05-06
-- INSERT INTO `orders` VALUES (99999,'VINET',1,'2023-11-10','2023-11-12','2023-11-11',1,0);
-- INSERT INTO `order_details` VALUES (99999,11,14,12,0)
-- (***I don't know why but the above line sometimes workes sometimes not, it says coloumn count doesn't match value count at row 1,
--  even though order_details has 5 coloumns and I am trying to insert 5***)
-- after testing new cell appeared in the log_book as well: new orderID: 99999
-- select * from analytical_layer;
-- it has the newly added row with orderID: 99999 and shipping date of 2023-11-11.
-- 2156 rows were returned. So our testing is successful.
-- delete FROM term1.order_details where orderID = 99999;
-- delete FROM orders where orderID = 99999;
    
-- ----------------------------------------
-- ----- Creating Data Marts as Views -----			
-- ----------------------------------------

-- 1. SALES ANALYSIS
-- 	A View/Mart containing information about sales

drop view if exists sales_analysis;
CREATE VIEW sales_analysis AS 
SELECT
        a.orderID AS OrderID,
        a.orderDate AS Ordered,
        WEEK(a.orderDate) AS WeekOfYear,
        MONTH(a.orderDate) AS MonthOfYear,
        a.category_name AS Category,
        a.productName AS Product,
        a.unitPrice_order_details AS Price,
        a.quantity_order_details AS Quantity,
        ROUND(SUM(a.unitPrice_order_details * a.quantity_order_details), 2) AS Revenue
        FROM
        analytical_layer a
		GROUP BY
        OrderID, Ordered, WeekOfYear, MonthOfYear, Category,Product,Price,Quantity;
-- 	1.4 Sales Trends Over Time
-- 		Just for this I created the view `sales_trends` to show how I would answer my analytical questions.
-- 		Added coloumn sales_ratio, which shows the monthly_revenue as percentage of the given year's revenue.
--  	Could look at most successful month in a year, in terms of share of revenue.
DROP VIEW IF EXISTS sales_trends;
CREATE VIEW sales_trends AS
	WITH MonthlySales AS (
		SELECT
			YEAR(Ordered) AS year,
			MONTH(Ordered) AS month,
			ROUND(SUM(Revenue), 2) AS monthly_revenue
		FROM
			sales_analysis
		GROUP BY
			year,
			month
	),
	YearlySales AS (
		SELECT
			YEAR(Ordered) AS year,
			ROUND(SUM(Revenue), 2) AS yearly_revenue
		FROM
			sales_analysis
		GROUP BY
			year
	)
	SELECT
		m.year,
		m.month,
		m.monthly_revenue,
		y.yearly_revenue,
		ROUND(m.monthly_revenue / y.yearly_revenue, 2) AS sales_ratio
	FROM
		MonthlySales m
	JOIN
		YearlySales y ON m.year = y.year;
    
-- 2. EMPLOYEE ANALYSIS
-- A View/Mart involving data about employees 

DROP VIEW IF EXISTS employee_analysis;
CREATE VIEW employee_analysis AS
WITH supervisor AS (
    SELECT
        DISTINCT e.employeeID,
        e.employeeName,
        e.employee_reportsTo AS supervisorID,
        s.employeeName AS supervisorName
    FROM
        analytical_layer e
        LEFT JOIN analytical_layer s ON e.employee_reportsTo = s.employeeID
),
total_sales AS (
    SELECT
        SUM(a.Revenue) AS totalRevenue
    FROM
        analytical_layer a
)
SELECT
    a.employeeID,
    a.employeeName AS trader,
    a.employee_title AS job_Title,
    a.employee_country AS country,
    a.employee_city AS city,
    s.supervisorName AS supervisor,
    COUNT(a.orderID) AS n_of_sales,
    ROUND(SUM(a.Revenue), 2) AS revenue,
    ROUND(SUM(a.Revenue) / ts.totalRevenue, 2) AS revenue_ratio
FROM
    analytical_layer a
    JOIN supervisor s ON a.employeeID = s.employeeID
    CROSS JOIN total_sales ts
GROUP BY
    a.employeeID, Trader, Job_Title, Country, City, Supervisor, ts.totalRevenue;
    
-- 3.  CUSTOMER ANALYSIS
-- A View/Mart involving data about customers

DROP VIEW IF EXISTS customer_analytics;
CREATE VIEW customer_analytics AS
SELECT
    customerID,
    customer_companyName AS Customer,
	MAX(customer_contactName) AS MainContact,
    MAX(customer_country) AS Country,
    MAX(customer_city) AS City,
    MIN(orderDate) AS FirstOrderDate,
    MAX(orderDate) AS LastOrderDate,
    DATEDIFF(CURRENT_TIME, MAX(orderDate)) as DaysSinceLastOrder,
    COUNT(DISTINCT orderID) AS OrderCount,
    ROUND(SUM(Revenue), 2) AS TotalRevenue,
    ROUND(SUM(Revenue) / (SELECT SUM(Revenue) FROM analytical_layer), 3) AS ShareOfRevenue
FROM
    analytical_layer
GROUP BY
    customerID, Customer;
    
-- 4. SHIPPING DESCRIPTIVE ANALYSIS
-- A view involving already shipped orders

DROP VIEW IF EXISTS descriptive_shipping_analytics;
Create view descriptive_shipping_analytics as
SELECT
        a.orderID AS OrderID,
        a.shippedDate AS Shipped,
        a.requiredDate AS Required,
        CASE WHEN DATEDIFF(a.shippedDate, a.requiredDate) >= 0
    THEN DATEDIFF(a.shippedDate, a.requiredDate)
    ELSE 0
END AS Delay,
        a.shipper_companyName AS Shipper,
        a.customer_country AS Country,
        a.customer_city AS City,
        ROUND(a.unitPrice_order_details * a.quantity_order_details, 2) AS Revenue,
        a.freight AS FreightCost
	FROM
        analytical_layer a
	WHERE 
        a.shippedDate IS NOT NULL; -- filter for already shipped orders;
        
-- 5. SHIPPING PREDICTIVE ANALYSIS
-- A view involving not yet shipped orders
DROP VIEW IF EXISTS predictive_shipping_analytics;
CREATE VIEW  predictive_shipping_analytics AS
    SELECT
        a.orderID AS OrderID,
        a.requiredDate AS Required,
        a.shipper_companyName AS Shipper,
        a.customer_country AS Country,
        a.customer_city AS City,
        ROUND(a.unitPrice_order_details * a.quantity_order_details, 2) AS Revenue,
        a.freight AS PredictedFreightCost
    FROM
        analytical_layer a
	WHERE
		a.shippedDate IS NULL; -- filter for not yet shipped orders;
