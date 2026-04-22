# ✅ Data Quality & Testing

## Approach

Data quality is enforced using dbt tests across three layers:

---

## 🔹 Structural Tests

- not_null
- unique
- relationships

---

## 🔹 Business Logic Tests

- Transaction reconciliation
- Inventory balance validation
- Customer age constraints

---

## 🔹 Example Rules

### Transaction Validation
transaction_total = sum(net_line_revenue)

### Inventory Balance
closing_stock = starting_stock + received_stock - sold_units - shrinkage_loss

### Customer Constraint
age >= 18

---

## 🔹 Data Freshness

- Transactions must be in the past
- Session end ≥ session start