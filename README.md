# Data Engineering 1 - Term project 1

**Péter Szilvási**
*CEU*
2023

This repository contains my materials for Term Project 1 for my Data Engineering 1 course.
## Introduction of the dataset

`Northwind` is a relational dataset about a fictitious gourmet food supplier, including sales & orders, customers, products, shippers, and employees.
Source is [Maven Analytics](https://mavenanalytics.io/data-playground?search=Northwind).
 - 7 relational tables
     - files are CSVs (76 kb)
 - 28 # of fields 
 - 2,985 # of records
 - contains some missing values
     - not yet shipped orders in `ShippedDate` column
     - employee reports to no one in case of Vice President Sales in `reportsTO` column
        
## 1. Database Setup
After downloading the files:
  1. I created the `EER diagram` to get a better overview about how the tables are linked.
  2. With Forward Engineer I created the Table Structures.
  3. Using `LOAD DATA INFILE` I created my Operational Layer.
     - `1_operational_data_layer.sql` uses data dumping for the sake of reproducibility
       
## 2. Analytical Plan
My plan is to look into the segments; sales, employee, customer, descriptive shipping, and predictive shipping data.
For easier querying an Analytical Layer would contain all relevant variables for the 5 segments.
I will implement a trigger to automatically update this layer in the event of new orders.
For more regular updates, I plan to use an event, scheduling the layer to update on a monthly basis. I would create this layer often so I will develop a stored procedure for efficiency.
Lastly for each of the 5 segments I will design a View/Mart that would specifically contain information of the segmented analysis.
My detailed questions can be found in `analytical_plan`.

## 3. Analytical Layer and ETL
The SQL script `2_analytical_codes.sql` contains the rest of the Project.
1. Created a Stored Procedure `CreateAnalyticalLayer` which would create table `analytical_layer` in a denormalized data structure.
3. Added Event `monthly_analytical_layer` for monthly updates.
    - this starts at the beginning of next month and is scheduled every 1 month
4. Added Trigger `after_order_insert`
    - each new order is logged in table `log_book`
    - trigger after insert on `order_details` inserts new row to `analytical_layer`

## 4. Marts as Views
For each of the 5 segments I created Marts as Views:
1. `sales_analysis` contains data about sales.
      - an additional view `sales_trends` shows how I would answer my detailed questions
2. `employee_analysis` contains data about employees.
3. `customer_analytics` contains data about customers.
4. `descriptive_shipping_analytics` contains data about past shipping orders
5. `predictive_shipping_analytics` contains data about future shipping orders
   
I demonstrated additional ETL elements in the querying of these views:
- `sales_analysis`
   - WEEK()
   - MONTH()
   - ROUND(SUM())
- `employee_analysis`
   - DISTINCT
   - ROUND(SUM()
   - COUNT()
   - common table expression (CTE)
- `customer_analytics`
   - MAX()
   - MIN()
   - DATEDIFF()
   - COUNT()
   - ROUND(SUM())
   - Nested querying
- `descriptive_shipping_analytics`
   - CASE WHEN
   - DATEDIFF()
   - ROUND()
- `predictive_shipping_analytics`
   - ROUND()
 
# 5. Reproducing
1. Download and run `1_operational_data_layer.sql` in MYSQL Workbench.
2. Download and run `2_analytical_codes.sql` in MYSQL Workbench.
