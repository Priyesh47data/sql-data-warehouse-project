 # 📊 Data Warehouse Project (SQL Server)

<div align="center">

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![ETL](https://img.shields.io/badge/ETL%20Pipeline-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![Medallion](https://img.shields.io/badge/Medallion%20Architecture-F2C811?style=for-the-badge&logo=databricks&logoColor=black)
![Star Schema](https://img.shields.io/badge/Star%20Schema-28A745?style=for-the-badge&logo=databricks&logoColor=white)

</div>

---

## 🚀 Overview

This project demonstrates the design and implementation of a **Data Warehouse using SQL Server**, following a **Medallion Architecture (Bronze → Silver → Gold)** approach.

The pipeline covers:
- 📥 Raw data ingestion (CSV → Bronze)
- 🧹 Data cleaning & transformation (Silver)
- ⭐ Dimensional modeling (Gold Layer)
- 🔍 Exploratory Data Analysis (EDA)

---

## 📸 Project Screenshots

<table>
  <tr>
    <td><img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1776630455/Screenshot_2026-04-20_015648_afkipr.png" width="140"/></td>
    <td><img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1776631004/Screenshot_2026-04-20_020607_hgjexw.png" width="280"/></td>
    <td><img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1776631004/Screenshot_2026-04-20_020607_hgjexw.png" width="360"/></td>
    <!-- <td><img src="screenshots/eda_queries.png" alt="EDA Pic" width="280"/></td> -->
  </tr>
  <tr>
    <td align="center"><b>🥉 Bronze Layer</b></td>
    <td align="center"><b>🥈 Silver Layer</b></td>
    <td align="center"><b>🥇 Gold Layer</b></td>
    <!-- <td align="center"><b>🔍 EDA Queries</b></td> -->
  </tr>
</table>

<br/>

<table>
  <tr>
    <td><img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1776630455/Screenshot_2026-04-20_015648_afkipr.png" width="580"/></td>
    <td><img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1776630455/Screenshot_2026-04-20_015614_ec1ti6.png" width="580"/></td>
  </tr>
  <tr>
    <td align="center"><b>⭐ Star Schema Design</b></td>
    <td align="center"><b>🔄 Data Flow Architecture</b></td>
  </tr>
</table>

---

## 🏗️ Architecture

```
Source CSV Files
       ↓
   Bronze Layer  (Raw Data — ingested as-is)
       ↓
   Silver Layer  (Cleaned & Transformed Data)
       ↓
   Gold Layer    (Star Schema — ready for analytics)
       ↓
   Analytics / EDA Queries
```

---

## 📁 Project Structure

```
data-warehouse-sql/
│
├── datasets/
│   ├── source_crm/
│   └── source_erp/
│
├── sql/
│   ├── bronze_layer.sql
│   ├── silver_layer.sql
│   ├── gold_layer.sql
│   └── eda_queries.sql
│
├── screenshots/
│   ├── bronze_layer.png
│   ├── silver_layer.png
│   ├── gold_layer.png
│   └── eda_queries.png
│
└── README.md
```

---

## 🥉 Bronze Layer — Raw Data

### 📌 Purpose
- Store raw data **as-is** from source systems (CRM & ERP)
- No transformation applied — single source of truth

### 📂 Tables

| Table | Source |
|---|---|
| `crm_cust_info` | CRM |
| `crm_prd_info` | CRM |
| `crm_sales_details` | CRM |
| `erp_cust_az12` | ERP |
| `erp_loc_a101` | ERP |
| `erp_px_cat_g1v2` | ERP |

### ⚙️ Key Features
- Bulk data loading using `BULK INSERT`
- Stored Procedure: `bronze.load_bronze`
- Load time tracking
- Error handling using `TRY...CATCH`

---

## 🥈 Silver Layer — Cleaned Data

### 📌 Purpose
- Data cleaning, transformation, and standardization

### 🔄 Transformations Applied

| Transformation | Description |
|---|---|
| Text Cleaning | Trim and clean all text fields |
| NULL Handling | Replace or flag NULL values |
| Standardization | Normalize gender & marital status |
| Date Fix | Correct invalid/incorrect dates |
| Deduplication | Remove duplicates using `ROW_NUMBER()` |
| Derived Columns | Extract product categories |
| Validation | Validate sales calculations |

### ⚙️ Key Features
- Stored Procedure: `silver.load_silver`
- Data validation logic
- Derived columns
- Data consistency improvements

---

## 🥇 Gold Layer — Business Model

### 📌 Purpose
- Create a **Star Schema** optimized for analytics and reporting

### ⭐ Schema Design

```
                  ┌─────────────────┐
                  │   dim_customers │
                  └────────┬────────┘
                           │
┌──────────────┐   ┌───────▼──────┐
│ dim_products ├───►  fact_sales  │
└──────────────┘   └──────────────┘
```

### 🔹 Dimension Tables

**`dim_customers`** — Customer demographics, country mapping, gender resolution (CRM + ERP)

**`dim_products`** — Product hierarchy (Category → Subcategory), cost & line, active products only

### 🔸 Fact Table

**`fact_sales`** — Orders, sales amount, quantity & price, foreign keys to all dimensions

---

## 🔍 Exploratory Data Analysis (EDA)

### 📈 Key Metrics

| Metric | Description |
|---|---|
| Total Sales | Overall revenue generated |
| Total Quantity | Units sold across all products |
| Average Price | Mean selling price |
| Total Orders | Number of distinct orders |
| Total Customers | Unique customer count |

### 📊 Business Insights

| Analysis | Detail |
|---|---|
| 💰 Revenue by Customer | Total sales per customer |
| 🌍 Sales by Country | Distribution across regions |
| 🏆 Top 5 Products | Highest revenue products |
| 📉 Bottom 5 Products | Lowest performing products |
| 👤 Top 10 Customers | Customers by revenue |
| 🔢 Low Order Customers | Customers with fewest orders |

---

## ⚙️ How to Run

### 1️⃣ Create the Database

```sql
CREATE DATABASE DataWarehouse;
USE DataWarehouse;
```

### 2️⃣ Run Scripts in Order

```sql
-- Step 1: Bronze Layer
-- Run: sql/bronze_layer.sql
EXEC bronze.load_bronze;

-- Step 2: Silver Layer
-- Run: sql/silver_layer.sql
EXEC silver.load_silver;

-- Step 3: Gold Layer
-- Run: sql/gold_layer.sql

-- Step 4: EDA Queries
-- Run: sql/eda_queries.sql
```

---
## EDA
 ```sql


                        /*======================================
                               Dimension Exploration
                         =======================================*/
--Explore all countries oue customers come from..
SELECT DISTINCT country FROM gold.dim_customers

--Explore all category "The major Dicisons"
SELECT 
	DISTINCT category,subcategory,product_name
FROM gold.dim_products
ORDER BY 1,2,3

                        /*======================================
                                Date Exploration
                         =======================================*/

 --Identify the Latest Order Date
Select MAX(order_date) Last_order FROM gold.fact_sales

  --Identify the Earliest Order Date 
Select MIN(order_date) Frist_order FROM gold.fact_sales


                        /*===================================================
                           Measure Exploration - Highest level of aggregation
                         ===================================================*/

/*
    Find the total sales
    Find how many items are sold
    Find the avg selling price
    Find the total numbers of orders
    Find the total numbers of products
    Find the total numbers of customers
    Find the total numbers of customers that has placed an order 
*/

SELECT 'Total Sales' measure_name,  SUM(sales_amount) measure_value  FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price' , AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total nr. Orders' , COUNT( DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total nr. Products' , COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total nr. Customers' , COUNT(customer_key) FROM gold.dim_customers



                        /*==================================
                            Magnitite Analysis
                         ===================================*/
--Compare the measure values by categories 
            -- It help us understand the importance of different categories


  -- What is the total revenue generated by each customers ? 
SELECT 
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(fs.sales_amount) Total_Sales
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales fs
    ON c.customer_key = fs.customer_key
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY SUM(fs.sales_amount) DESC

  -- What is the distribution of sold items across countries ?

SELECT 
    c.country,
    SUM(fs.quantity) Total_sold_items
FROM gold.fact_sales fs
    LEFT JOIN gold.dim_customers c
ON fs.customer_key = c.customer_key
GROUP BY c.country
ORDER BY Total_sold_items DESC


                        /*==================================
                           Ranking Analysis
                         ===================================*/

  -- Which 5 product generated the highest revenue?
SELECT
*
FROM ( 
        SELECT
            p.product_name,
            SUM(fs.sales_amount) total_sales,
            ROW_NUMBER() OVER(ORDER BY SUM(fs.sales_amount) DESC) rank_prodcuts
        FROM gold.fact_sales fs
        LEFT JOIN gold.dim_products p
            ON fs.product_key = p.product_key
        GROUP BY p.product_name
        )t
WHERE rank_prodcuts <= 5
  -- What are the 5 worst-performing prodcuts in terms of sales?

  
SELECT TOP 5
    p.product_name,
    SUM(fs.sales_amount) total_sales,
    ROW_NUMBER() OVER( ORDER BY SUM(fs.sales_amount) ) rank_prodcuts
FROM gold.fact_sales fs
    LEFT JOIN gold.dim_products p
ON fs.product_key = p.product_key
GROUP BY p.product_name


  -- SQL TASK:- Find Top-10 customers who have generated the highest revenue
                -- And customers with the fewest order placed

SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(fs.sales_amount) Total_Sales
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales fs
    ON c.customer_key = fs.customer_key
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY Total_Sales DESC
--=====================================--
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(fs.order_number) Total_Orders
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales fs
    ON c.customer_key = fs.customer_key
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY Total_Orders 
```
---
## 🧠 Key Concepts Used

| Concept | Usage |
|---|---|
| Data Warehousing | Central repository for analytics |
| ETL Pipeline | Extract → Transform → Load |
| Medallion Architecture | Bronze / Silver / Gold layers |
| Star Schema | Fact + Dimension tables |
| Window Functions | `ROW_NUMBER()`, `LEAD()` |
| Stored Procedures | Modular, reusable SQL logic |
| Data Validation | Quality checks at each layer |
| Error Handling | `TRY...CATCH` blocks |

---

## 📌 Highlights

✅ End-to-end Data Warehouse implementation  
✅ Real-world ETL pipeline design  
✅ Clean and modular SQL code  
✅ Business-driven analytics  
✅ Scalable Medallion Architecture  

---

 

## 👨‍💻 Author

**Priyesh**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/priyesh50)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/Priyesh47data)

- 🎓 BCA Student | Data Analyst
- 🛠️ Skills: SQL · Power BI · Excel · Data Warehousing

---

<div align="center">

⭐ **If you found this project helpful, please give it a star!** ⭐

</div>
