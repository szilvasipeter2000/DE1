# Data Engineering 1 - Term project 1

**Péter Szilvási**
*CEU*
2023

This repository contains my materials for Term Project 1 for my Data Engineering 1 course.
## Introduction of the dataset
`Northwind` is a relational dataset about a fictitious gourmet food supplier, including sales & orders, customers, products, shippers, and employees.
Source is [Maven Analytics](https://mavenanalytics.io/data-playground?search=Northwind).
 - 7 relatioanal tables
 - 28 # of fields 
 - 2,985 # of records
 - files are CSVs (76 kb)
 - contains some missing values
      - not yet shipped orders in `ShippedDate` column
      - employee reports to no one in case of Vice President Sales in `reportsTO` column
## 1. Database Setup
After downloading the files:
  1. I created the `EER diagram` to get a better overview about how the tables are linked.
  2. With Forward Engineer I created the Table Structures.
  3. Using `LOAD DATA INFILE` I created my Operational Layer.
     - `operational_data_layer.sql` uses data dumping for the sake of reproducibility
