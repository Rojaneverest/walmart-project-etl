# Walmart Data Warehouse - Business Requirements

## Overview
This document outlines the key business requirements, critical business questions, and Key Performance Indicators (KPIs) that the Walmart data warehouse is designed to address. These requirements guide the design and implementation of the ETL processes and dimensional model.

## Critical Business Questions

The data warehouse enables Walmart to explore the following critical business questions:

### 1. Sales Performance Analysis
**Business Question:** How are sales performing across different dimensions (time, location, product categories) and how do they compare to historical trends?

**Value:** Identifies high and low-performing areas, seasonal patterns, and growth opportunities.

**Dimensions to Analyze:** Time (day, month, quarter, year), Store, Product Category, Customer Segment, Region

### 2. Inventory Optimization
**Business Question:** What are the optimal inventory levels for each product category across different stores to minimize stockouts while avoiding excess inventory?

**Value:** Reduces carrying costs, minimizes lost sales opportunities, and improves capital efficiency.

**Dimensions to Analyze:** Product, Store, Time, Stock Levels, Reorder Points, Sales Velocity

### 3. Product Category Performance
**Business Question:** Which product categories and subcategories drive the most profit, and how does their performance vary by region and customer segment?

**Value:** Enables strategic merchandising decisions and targeted promotions.

**Dimensions to Analyze:** Product Category, Product Subcategory, Region, Customer Segment, Profit Margin

### 4. Return Rate Analysis
**Business Question:** What are the primary reasons for product returns, and which products, categories, or stores experience abnormally high return rates?

**Value:** Identifies quality issues, improves product selection, and enhances customer satisfaction.

**Dimensions to Analyze:** Return Reason, Product, Store, Time, Customer Segment, Return Rate

### 5. Customer Segmentation Effectiveness
**Business Question:** How do different customer segments contribute to sales and profitability, and what are their purchasing patterns?

**Value:** Enables targeted marketing, personalized promotions, and improved customer experience.

**Dimensions to Analyze:** Customer Segment, Purchase Frequency, Average Order Value, Product Categories, Time

### 6. Supply Chain Efficiency
**Business Question:** How efficiently are suppliers fulfilling orders, and what is the impact on inventory levels and stockouts?

**Value:** Improves supplier relationships, reduces lead times, and enhances inventory management.

**Dimensions to Analyze:** Supplier, Lead Time, Fill Rate, Product Category, Store, Stockout Frequency

### 7. Promotional Effectiveness
**Business Question:** How do various promotional activities impact sales, profit margins, and inventory turnover across different product categories and store locations?

**Value:** Optimizes marketing spend and improves ROI on promotional activities.

**Dimensions to Analyze:** Promotion Type, Product, Store, Time, Sales Lift, Margin Impact

## Key Performance Indicators (KPIs)

The following KPIs are implemented in the data warehouse to measure business performance:

### Sales Performance KPIs
1. **Total Sales Revenue**
   - Definition: Sum of all sales amounts
   - Formula: `SUM(sales_amount)`
   - Granularity: Can be aggregated by day, month, quarter, year, store, region, product category

2. **Sales Growth**
   - Definition: Percentage change in sales compared to previous period
   - Formula: `(Current Period Sales - Previous Period Sales) / Previous Period Sales * 100`
   - Granularity: Month-over-Month, Quarter-over-Quarter, Year-over-Year

3. **Average Transaction Value**
   - Definition: Average amount spent per transaction
   - Formula: `SUM(sales_amount) / COUNT(DISTINCT order_id)`
   - Granularity: By store, region, customer segment, time period

4. **Gross Profit Margin**
   - Definition: Profit as a percentage of sales
   - Formula: `SUM(profit) / SUM(sales_amount) * 100`
   - Granularity: By product, category, store, region, time period

### Inventory Management KPIs
5. **Inventory Turnover Rate**
   - Definition: Number of times inventory is sold and replaced in a period
   - Formula: `Cost of Goods Sold / Average Inventory Value`
   - Granularity: By product, category, store, region, time period

6. **Days of Supply**
   - Definition: Number of days the current inventory will last based on average daily sales
   - Formula: `Current Inventory / Average Daily Sales`
   - Granularity: By product, category, store, region

7. **Stockout Rate**
   - Definition: Percentage of time items are out of stock
   - Formula: `Number of Days Item Out of Stock / Total Number of Days in Period * 100`
   - Granularity: By product, category, store, region

8. **Inventory Accuracy**
   - Definition: Percentage match between physical inventory and system records
   - Formula: `(1 - (|Physical Count - System Count| / Physical Count)) * 100`
   - Granularity: By store, region, product category

### Customer Metrics KPIs
9. **Customer Retention Rate**
   - Definition: Percentage of customers who made repeat purchases
   - Formula: `Number of Repeat Customers / Total Number of Customers * 100`
   - Granularity: By time period, customer segment, region

10. **Customer Lifetime Value (CLV)**
    - Definition: Total value a customer generates over their relationship with Walmart
    - Formula: `Average Purchase Value × Purchase Frequency × Average Customer Lifespan`
    - Granularity: By customer segment, region, acquisition channel

### Product Quality KPIs
11. **Return Rate**
    - Definition: Percentage of sold items that are returned
    - Formula: `SUM(quantity_returned) / SUM(order_quantity) * 100`
    - Granularity: By product, category, store, region, reason code, time period

12. **Return Amount Ratio**
    - Definition: Value of returns as a percentage of sales
    - Formula: `SUM(return_amount) / SUM(sales_amount) * 100`
    - Granularity: By product, category, store, region, time period

13. **Days to Return**
    - Definition: Average number of days between purchase and return
    - Formula: `AVG(days_since_sale)` for returned items
    - Granularity: By product, category, reason code

### Operational Efficiency KPIs
14. **Sell-Through Rate**
    - Definition: Percentage of inventory sold in a specific time period
    - Formula: `Units Sold / (Beginning Inventory + New Inventory) * 100`
    - Granularity: By product, category, store, time period

15. **Profit per Square Foot**
    - Definition: Profit generated per square foot of retail space
    - Formula: `Total Profit / Total Square Footage`
    - Granularity: By store, region, department

## Implementation Notes

- All KPIs should be available through the Power BI dashboards with appropriate visualizations
- KPIs should support drill-down capabilities from summary to detailed views
- Historical trending should be available for all KPIs
- Benchmarking against targets should be implemented where applicable
- Alerts should be configurable for KPIs that fall outside acceptable thresholds
