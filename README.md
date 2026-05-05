# 🏨 Hotel Booking Analysis | End-to-End Data Engineering on Snowflake

![Snowflake](https://img.shields.io/badge/Platform-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/Language-SQL-orange?style=for-the-badge&logo=postgresql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen?style=for-the-badge)
![Dataset](https://img.shields.io/badge/Records-1,995-blue?style=for-the-badge)

---

## 📌 Project Summary

| Item | Detail |
|------|--------|
| **Domain** | Hospitality / Hotel Management |
| **Platform** | Snowflake Cloud Data Warehouse |
| **Raw Records** | 2,000 bookings |
| **Clean Records** | 1,995 bookings |
| **Total Revenue** | $661,194.63 |
| **Date Range** | Nov 2024 → May 2026 (18 months) |
| **Approach** | Ingestion → Cleaning → Transformation → Analytics |
| **Queries** | 16 analytical SQL queries |

---

## 🏗️ Architecture — Medallion-Style Pipeline

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                        END-TO-END DATA PIPELINE                                │
│                                                                                │
│  ┌──────────┐    ┌───────────────┐    ┌──────────────┐    ┌───────────────┐   │
│  │  BRONZE  │───▶│    SILVER     │───▶│     GOLD     │───▶│   INSIGHTS    │   │
│  │ (Raw)    │    │ (Cleaned)     │    │ (Enriched)   │    │ (Analytics)   │   │
│  └──────────┘    └───────────────┘    └──────────────┘    └───────────────┘   │
│                                                                                │
│  • CSV Ingest     • Deduplication     • Date casting      • 16 queries        │
│  • 2,000 rows     • Null handling     • Typo correction   • KPIs & trends     │
│  • Raw schema     • Invalid removal   • ABS() fix         • Recommendations   │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
hotel-booking-data-engineering/
│
├── 📄 README.md                   ← Project documentation 
├── 📄 hotel_booking.sql           ← All SQL queries (16 analyses)
│
├── 🔹 Bronze Layer                ← Raw data ingestion (HOTEL_BOOKING)
├── 🔹 Silver Layer                ← Cleaned data (HOTEL_BOOKING_CLEANED)
├── 🔹 Gold Layer                  ← Derived metrics & transformations
└── 🔹 Analytics Layer             ← Business insights & KPIs
```

---

## 📊 Data Schema

| # | Column | Data Type | Description |
|---|--------|-----------|-------------|
| 1 | `BOOKING_ID` | VARCHAR | Unique booking identifier |
| 2 | `HOTEL_ID` | NUMBER | Hotel identifier |
| 3 | `HOTEL_CITY` | VARCHAR | City where hotel is located |
| 4 | `CUSTOMER_ID` | VARCHAR | Customer identifier |
| 5 | `CUSTOMER_NAME` | VARCHAR | Customer full name |
| 6 | `CUSTOMER_EMAIL` | VARCHAR | Customer email address |
| 7 | `CHECK_IN_DATE` | DATE | Check-in date (converted from VARCHAR) |
| 8 | `CHECK_OUT_DATE` | DATE | Check-out date (converted from VARCHAR) |
| 9 | `ROOM_TYPE` | VARCHAR | Suite / Deluxe / Standard |
| 10 | `NUM_GUESTS` | NUMBER | Number of guests per booking |
| 11 | `TOTAL_AMOUNT` | NUMBER | Booking amount in USD/INR/EUR |
| 12 | `CURRENCY` | VARCHAR | USD / INR / EUR |
| 13 | `BOOKING_STATUS` | VARCHAR | Confirmed / Cancelled / No-Show |

---

## 🔧 Pipeline Execution Steps

### 🟤 Step 1 — Bronze Layer (Raw Ingestion)
```sql
SELECT * FROM SASWATDB.PUBLIC.HOTEL_BOOKING;
-- 2,000 raw booking records loaded
```

### ⚪ Step 2 — Silver Layer (Data Quality & Cleaning)

| Check | Method | Result |
|-------|--------|--------|
| Duplicate Removal | `ROW_NUMBER()` on BOOKING_ID | 2,000 → 1,995 rows |
| Date Format Fix | `MM/DD/YYYY` → DATE type | ✅ Converted |
| Typo Correction | `'Confirmeeed'` → `'Confirmed'` | ✅ Fixed |
| Null Handling | Replaced with defaults (Unknown, 0) | ✅ Clean |
| Invalid Records | Removed HOTEL_ID = 0, unknown customers | ✅ Removed |
| Negative Amounts | Applied `ABS()` | ✅ Corrected |

### 🟡 Step 3 — Gold Layer (Transformation)
```sql
-- Created clean table with all fixes applied
CREATE TABLE HOTEL_BOOKING_CLEANED AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY BOOKING_ID ORDER BY ...) AS rn
    FROM HOTEL_BOOKING
) WHERE rn = 1;
```

### 🟢 Step 4 — Analytics Layer (Insights & KPIs)
- 16 analytical queries covering revenue, cancellations, trends, and customer behavior

---

## 📈 Key Performance Indicators (KPIs)

| KPI | Value | Interpretation |
|-----|-------|----------------|
| 🏨 Total Bookings | **1,995** | After cleaning |
| 💰 Total Revenue | **$661,194.63** | All currencies combined |
| 💵 Avg Revenue/Booking | **$331.43** | Per-booking average |
| 📅 Date Range | **18 months** | Nov 2024 → May 2026 |
| ❌ Non-Confirmed Rate | **59.1%** | ⚠️ Critical issue |
| 🔁 Repeat Customers | **0** | Zero loyalty |

---

## 🔍 Deep-Dive Insights

### 1️⃣ Booking Status — ⚠️ Critical Problem

| Status | Count | Percentage | Revenue Impact |
|--------|-------|------------|----------------|
| ✅ Confirmed | 813 | 40.75% | Revenue realized |
| ❌ Cancelled | 621 | 31.13% | Revenue lost |
| 🚫 No-Show | 558 | 27.97% | Revenue at risk |

> ⚠️ **ALERT: ~59% of all bookings are NOT confirmed.** This is a critical operational and revenue problem. Only 4 in 10 bookings result in actual stays.

---

### 2️⃣ Room Types — No Premium Differentiation

| Room Type | Bookings | Avg Amount | Share |
|-----------|----------|------------|-------|
| Suite | 670 | $332.70 | 33.6% |
| Deluxe | 662 | $334.23 | 33.2% |
| Standard | 661 | $327.26 | 33.1% |

> 💡 **Insight:** All three room types are identically priced (~$330). **No premium positioning** for Suite/Deluxe — missed revenue opportunity.

---

### 3️⃣ Revenue by Currency — Balanced Portfolio

| Currency | Bookings | Revenue | Share |
|----------|----------|---------|-------|
| 💵 USD | 706 | $229,818 | 34.8% |
| 🇮🇳 INR | 654 | $220,796 | 33.4% |
| 💶 EUR | 635 | $210,581 | 31.8% |

> 💡 **Insight:** Perfectly balanced across 3 currencies — global customer base with no regional dependency.

---

### 4️⃣ Monthly Booking Trends

| Month | Bookings | Revenue | Notes |
|-------|----------|---------|-------|
| 📈 May 2025 | 128 | $43K | Peak month |
| 📊 Jan 2026 | ~100 | $40.3K | Revenue bump |
| 📉 Average | ~100-128 | ~$33K | Consistent |

> 💡 **Insight:** No strong seasonality — bookings are consistent at ~100-128/month with minor peaks.

---

### 5️⃣ Customer Behavior — ⚠️ Zero Loyalty

| Metric | Value |
|--------|-------|
| Unique Customers | 1,995 |
| Repeat Customers | **0** |
| Repeat Rate | **0%** |
| Unique Cities | ~1,000+ |
| Top City | East Michael (6 bookings) |
| Highest Avg Spend | Lake John ($526.51/booking) |

> ⚠️ **ALERT: Zero repeat customers** — every single customer booked only once. No loyalty program, no retention, no recurring revenue.

---

### 6️⃣ City Distribution — Highly Fragmented

| Rank | City | Bookings | Revenue |
|------|------|----------|---------|
| 1 | East Michael | 6 | $2,337 |
| 2 | Lake John | — | Highest avg ($526.51) |
| — | 1,000+ other cities | 1-2 each | Scattered |

> 💡 **Insight:** Bookings are spread across 1,000+ cities with no concentration. No "home market" advantage.

---

## 📋 Complete Analysis Catalog

| # | Analysis | Category | Layer |
|---|----------|----------|-------|
| 1 | Row count, date range, total revenue | Overview | Analytics |
| 2 | Booking status distribution | Distribution | Analytics |
| 3 | Room type distribution | Distribution | Analytics |
| 4 | Currency-wise revenue split | Distribution | Analytics |
| 5 | Top 10 cities by bookings & revenue | Aggregation | Analytics |
| 6 | Monthly booking trend | Time Series | Analytics |
| 7 | Avg stay duration by room type | Aggregation | Analytics |
| 8 | Avg revenue per booking (room × currency) | Cross-Tab | Analytics |
| 9 | Cancellation & No-Show rate by city | Risk | Analytics |
| 10 | Cancellation rate by month | Trend | Analytics |
| 11 | Revenue per guest by room type | Aggregation | Analytics |
| 12 | Repeat customer analysis | Customer | Analytics |
| 13 | Month-over-month revenue growth (%) | Growth | Analytics |
| 14 | Running total revenue over time | Cumulative | Analytics |
| 15 | City ranking: cancellation rate vs volume | Risk | Analytics |
| 16 | Customer booking frequency distribution | Customer | Analytics |

---

## 🛠️ SQL Techniques & Methods

| Technique | Use Case |
|-----------|----------|
| `ROW_NUMBER() OVER (PARTITION BY)` | Deduplication |
| `TO_DATE(col, 'MM/DD/YYYY')` | Date format conversion |
| `REPLACE()` | Typo correction (Confirmeeed → Confirmed) |
| `COALESCE() / NVL()` | Null replacement with defaults |
| `ABS()` | Fix negative amounts |
| `DATEDIFF()` | Stay duration calculation |
| `SUM() OVER (ORDER BY)` | Running total (window function) |
| `LAG()` | Month-over-month growth |
| `ROUND(x * 100.0 / total, 2)` | Percentage calculation |
| `GROUP BY + HAVING` | Repeat customer detection |
| `CASE WHEN` | Conditional aggregation |

---

## ✅ Key Findings & Recommendations

| # | Finding | Recommendation | Priority |
|---|---------|----------------|----------|
| 1 | 59% cancellation + no-show rate | Implement prepayment, deposit policies, booking reminders | 🔴 High |
| 2 | Zero repeat customers | Launch loyalty program, re-engagement campaigns, member discounts | 🔴 High |
| 3 | No room pricing differentiation | Add premium pricing for Suite/Deluxe with value-added services | 🟡 Medium |
| 4 | 1,000+ fragmented cities | Identify top 10-20 cities, focus marketing spend there | 🟡 Medium |
| 5 | Consistent monthly bookings | Introduce off-peak discounts to create growth above 128/month | 🟢 Low |
| 6 | Balanced currency mix | Maintain global presence, add localized payment options | 🟢 Low |

---

## 🖥️ Tech Stack

| Layer | Technology |
|-------|------------|
| **Cloud Platform** | Snowflake |
| **Compute** | COMPUTE_WH (Virtual Warehouse) |
| **Database** | SASWATDB |
| **Schema** | PUBLIC |
| **Language** | SQL |
| **Role** | DATA_ANALYST |
| **IDE** | Snowsight (Snowflake Web UI) |

---

## 🚀 How to Run

```sql
-- Step 1: Set context
USE DATABASE SASWATDB;
USE SCHEMA PUBLIC;

-- Step 2: Verify raw data
SELECT * FROM HOTEL_BOOKING LIMIT 10;

-- Step 3: Verify clean data
SELECT * FROM HOTEL_BOOKING_CLEANED LIMIT 10;

-- Step 4: Run hotel_booking.sql queries sequentially
```
