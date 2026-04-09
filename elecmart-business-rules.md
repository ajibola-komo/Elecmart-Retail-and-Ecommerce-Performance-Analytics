# Elecmart — Business Rules & Analytics Framework

**Last Updated:** April 2026

---

## 1. Executive Summary

This document defines the business logic, data generation framework, dimensional modelling approach, and analytics definitions for the Elecmart project.

Elecmart simulates a modern retail and e-commerce business, enabling analysis across:

- Sales & profitability  
- Customer behavior  
- Marketing performance  
- Inventory management  

The framework is designed to:

- Reflect real-world retail operations  
- Support scalable analytics in Snowflake  
- Power Tableau dashboards (executive + operational)  

---

## 2. Business Definitions (Source of Truth)

This section defines how the business interprets data, independent of implementation.

---

### 2.1 Transactions & Sales

- A transaction represents a single purchase event  
- A sale contains multiple line items (products)  
- Transactions can occur via:
  - Online (Web / Mobile)
  - Physical store  

**Transaction statuses:**
- `Completed` → successful sale  
- `Returned` → reversal of a completed sale  

---

### 2.2 Revenue

- **Total Revenue:** Total value of goods sold after discounts  
- **Net Revenue:** Revenue after discounts and returns  
- Returned transactions reduce total revenue  

---

### 2.3 Customer

A customer is an individual who interacts with the business.

Customers may be:
- Registered (identified) → linked to `customer_id`
- Guest (anonymous) → session-only interaction  

---

### 2.4 Promotions & Campaigns

- A promotion represents a discount applied to a transaction  
- A campaign represents a marketing effort that may or may not include a promotion  
- A transaction can have at most one promotion  
- Campaigns drive traffic, engagement, and conversion  

---

### 2.5 Inventory

Inventory reflects stock levels per product × store × time period.

Inventory is tracked using **monthly snapshots**.

---

## 3. Data Generation Framework (Simulation Rules)

These are simulation rules, not business definitions.

---

### 3.1 General

- Data generated using Python (Faker, NumPy)
- Deterministic via random seed
- Stored as Parquet → ingested into Snowflake
- Business constraints enforced at generation time  

---

### 3.2 Dates

- Simulation window: **30 May 2001 → Present (~20+ years)**
- Date keys: `YYYYMMDD`
- All fact records reference valid date keys  

---

### 3.3 Customers

- Total customers: **50,000**
- Minimum age: **18 years**
- Each customer assigned a persona  

Personas drive:
- Purchase frequency  
- Average order value (AOV)  
- Loyalty tier  

---

### 3.4 Products

- ~470 SKUs across:
  - 10 categories  
  - 28 subcategories  
  - ~50 brands  

Rules:
- `unit_price > unit_cost`
- Valid category hierarchy enforced
- Price segments defined (Low → Flagship)

---

### 3.5 Stores

- Total stores: **50**
- Store types: Mall, Standalone, Outlet, Warehouse  
- Opening date precedes transactions  

---

### 3.6 Transactions & Sales

- ~400,000 transactions  
- ~1.6M line items  

Rules:
- Each transaction references:
  - store_id
  - transaction_date_id  
- Optional:
  - customer_id (registered only)
  - session_id (online only)

Calculations:
```sql
transaction_total = transaction_subtotal - transaction_discount_applied
line_total = unit_price * quantity
line_cost = unit_cost * quantity