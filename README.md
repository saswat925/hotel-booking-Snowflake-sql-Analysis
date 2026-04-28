# 🏨 Hotel Booking Analysis (Snowflake SQL Project)

## 📌 Project Overview

This project analyzes a hotel booking dataset using **Snowflake SQL** to identify revenue patterns, booking behavior, and business performance insights.

* **Total Bookings:** 1,995 (cleaned from 2,000 raw rows)
* **Date Range:** Nov 25, 2024 → May 26, 2026 (~18 months)
* **Total Revenue:** $661,194.63
* **Average Revenue per Booking:** $331.43

---

## 🧠 Key Insights

### 1. 📉 Booking Status Analysis (Major Issue)

| Status    | Count | %      |
| --------- | ----- | ------ |
| Confirmed | 813   | 40.75% |
| Cancelled | 621   | 31.13% |
| No-Show   | 558   | 27.97% |

👉 Around **59% bookings are NOT confirmed** (Cancelled + No-Show)

⚠️ This indicates a major revenue leakage problem.

---

### 2. 🛏️ Room Type Analysis

* Suite: 670 bookings
* Deluxe: 662 bookings
* Standard: 661 bookings

👉 Almost equal distribution across all room types
👉 Avg revenue per booking: ~$327–$334

⚠️ No pricing differentiation strategy is visible

---

### 3. 💱 Currency Distribution

| Currency | Bookings | Revenue  |
| -------- | -------- | -------- |
| USD      | 706      | $229,818 |
| INR      | 654      | $220,796 |
| EUR      | 635      | $210,581 |

👉 Revenue is evenly distributed across currencies

---

### 4. 🌍 City Insights

* East Michael: Highest bookings (6), $2,337 revenue
* Lake John: Highest avg booking value ($526.51)

⚠️ Over 1,000+ unique cities
⚠️ Most cities have only 1–2 bookings → highly fragmented demand

---

### 5. 📅 Monthly Trends

* Peak Month: May 2025 → 128 bookings, $43K revenue
* Lowest Month: Nov 2024 → 18 bookings
* Stable trend: ~100–128 bookings/month

👉 No strong seasonality observed

---

### 6. 🔁 Customer Retention

* **Zero repeat customers found**

⚠️ Indicates:

* Either only acquisition is happening
* OR retention/loyalty is very weak

---

## 🚨 Business Recommendations

### 🔴 1. Reduce Cancellation & No-Show (Top Priority)

* Introduce prepayment system
* Automated reminders (SMS/Email)
* Overbooking strategy for optimization

### 🟡 2. Improve Customer Retention

* Loyalty program
* Discount for repeat bookings
* Re-engagement campaigns

### 🟠 3. Pricing Strategy Improvement

* Currently flat pricing across room types
* Introduce premium pricing for Suite/Deluxe

### 🔵 4. Market Focus Strategy

* Too many fragmented cities
* Identify top performing cities for targeted marketing

---

## 🛠️ Tools Used

* ❄️ Snowflake SQL (Data Cleaning + Analysis)
* 📊 SQL Window Functions
* 📈 Aggregations & Time Series Analysis

---

## 📂 Files in Project

* `hotel_booking.sql` → All SQL queries (Snowflake)
* `README.md` → Project documentation (this file)

---

## 📌 Conclusion

This analysis shows that the biggest business risk is **high cancellation/no-show rate and zero customer retention**, while pricing strategy remains under-optimized.

---

⭐ If improved, this dataset can drive strong revenue optimization and customer growth strategies.
