# 📦 Dimension Tables

---

## dim_customer

**Grain:** One row per customer

**Description:**  
Stores customer demographics, acquisition channel, loyalty status, and marketing consent.

### Key Columns

| Column | Description |
|------|------------|
| customer_id | Primary key |
| email_address | Unique identifier |
| age | Customer age |
| customer_persona | Behavioral segment |
| signup_date | Registration date |

---

## dim_product

**Grain:** One row per product (SKU)

**Description:**  
Product master data including category, pricing, and brand.

### Key Columns

| Column | Description |
|------|------------|
| product_id | Primary key |
| category_id | Product category |
| brand_id | Manufacturer |
| unit_price | Selling price |
| unit_cost | Cost |

---

## dim_store

**Grain:** One row per store

**Description:**  
Physical store attributes including size, type, and location.

---

## dim_location

**Grain:** One row per location

**Description:**  
Geographic reference for stores and customers.

---

## dim_date

**Grain:** One row per date

**Description:**  
Calendar table supporting time-based analysis.

---

## dim_campaign

**Grain:** One row per campaign

---

## dim_promotion

**Grain:** One row per promotion