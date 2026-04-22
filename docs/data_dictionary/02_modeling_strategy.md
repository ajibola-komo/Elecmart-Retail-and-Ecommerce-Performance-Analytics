# Data Modeling Strategy

## Approach

| Layer   | Modeling Style |
|--------|--------------|
| Bronze | Raw relational |
| Silver | Clean relational |
| Gold   | Star schema |

---

## Design Principles

- Clear grain definition per table
- Conformed dimensions
- Fact tables optimized for analytics
- Denormalization in Gold for BI performance

---

## Fact Tables

- fact_sale → Line-level transactions
- fact_transaction → Transaction summary
- fact_inventory → Monthly snapshots
- fact_clickstream → Session-level data

---

## Dimension Tables

- Customer, Product, Store, Campaign, Promotion, Location, Date

---

## Future Enhancements

- SCD Type 2 implementation for:
  - Customers
  - Product pricing
  - Store attributes