# 📊 Fact Tables

---

## 💰 fact_sale

**Grain:** One row per product per transaction

**Description:**  
Captures detailed sales data including quantity, revenue, cost, and discounts.

### Key Metrics

- line_total
- allocated_line_discount
- net_line_revenue

---

## 🧾 fact_transaction

**Grain:** One row per transaction

**Description:**  
Aggregated transaction-level data including totals, discounts, and payment details.

---

## 📦 fact_inventory

**Grain:** One row per product per store per month

**Description:**  
Tracks stock movement and inventory levels.

---

## 🌐 fact_clickstream

**Grain:** One row per session

**Description:**  
Captures user behavior on the website including browsing and purchase actions.