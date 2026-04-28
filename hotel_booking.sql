USE ROLE DATA_ANALYST;

CREATE OR REPLACE TABLE SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY BOOKING_ID ORDER BY CHECK_OUT_DATE DESC) AS rn
    FROM SASWATDB.PUBLIC.HOTEL_BOOKING
    WHERE BOOKING_ID IS NOT NULL
)
SELECT
    BOOKING_ID,
    COALESCE(HOTEL_ID, 0) AS HOTEL_ID,
    COALESCE(HOTEL_CITY, 'Unknown') AS HOTEL_CITY,
    COALESCE(CUSTOMER_ID, 'Unknown') AS CUSTOMER_ID,
    COALESCE(CUSTOMER_NAME, 'Unknown') AS CUSTOMER_NAME,
    CASE WHEN CUSTOMER_EMAIL = 'invalid-email' OR CUSTOMER_EMAIL IS NULL THEN 'unknown@placeholder.com' ELSE CUSTOMER_EMAIL END AS CUSTOMER_EMAIL,
    COALESCE(TRY_TO_DATE(CHECK_IN_DATE, 'MM/DD/YYYY'), CHECK_OUT_DATE) AS CHECK_IN_DATE,
    COALESCE(CHECK_OUT_DATE, TRY_TO_DATE(CHECK_IN_DATE, 'MM/DD/YYYY')) AS CHECK_OUT_DATE,
    COALESCE(ROOM_TYPE, 'Unknown') AS ROOM_TYPE,
    COALESCE(NUM_GUESTS, 1) AS NUM_GUESTS,
    ABS(COALESCE(TOTAL_AMOUNT, 0)) AS TOTAL_AMOUNT,
    COALESCE(CURRENCY, 'USD') AS CURRENCY,
    CASE
        WHEN BOOKING_STATUS = 'Confirmeeed' THEN 'Confirmed'
        WHEN BOOKING_STATUS IS NULL OR TRIM(BOOKING_STATUS) = '' THEN 'Unknown'
        ELSE BOOKING_STATUS
    END AS BOOKING_STATUS
FROM deduped
WHERE rn = 1;

select * from hotel_booking_cleaned  

DELETE FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED WHERE HOTEL_ID = 0 OR CUSTOMER_ID = 'Unknown';

-- ============================================================
-- LEVEL 1: BASIC (Descriptive)
-- ============================================================

-- 1. Row count, date range, total revenue
SELECT
    COUNT(*) AS total_bookings,
    MIN(CHECK_IN_DATE) AS earliest_checkin,
    MAX(CHECK_IN_DATE) AS latest_checkin,
    SUM(TOTAL_AMOUNT) AS total_revenue,
    ROUND(AVG(TOTAL_AMOUNT), 2) AS avg_revenue_per_booking
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED;

-- 2. Booking status distribution
SELECT
    BOOKING_STATUS,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY BOOKING_STATUS
ORDER BY cnt DESC;

-- 3. Room type distribution
SELECT
    ROOM_TYPE,
    COUNT(*) AS cnt,
    ROUND(AVG(TOTAL_AMOUNT), 2) AS avg_amount,
    ROUND(AVG(NUM_GUESTS), 1) AS avg_guests
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY ROOM_TYPE
ORDER BY cnt DESC;

-- 4. Currency-wise revenue split
SELECT
    CURRENCY,
    COUNT(*) AS bookings,
    SUM(TOTAL_AMOUNT) AS total_revenue,
    ROUND(AVG(TOTAL_AMOUNT), 2) AS avg_amount
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY CURRENCY
ORDER BY total_revenue DESC;


---aggregation and trends

-- 5. Top 10 cities by bookings & revenue
SELECT
    HOTEL_CITY,
    COUNT(*) AS bookings,
    SUM(TOTAL_AMOUNT) AS total_revenue,
    ROUND(AVG(TOTAL_AMOUNT), 2) AS avg_amount
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY HOTEL_CITY
ORDER BY bookings DESC
LIMIT 10;

-- 6. Monthly booking trend
SELECT
    DATE_TRUNC('MONTH', CHECK_IN_DATE) AS booking_month,
    COUNT(*) AS bookings,
    SUM(TOTAL_AMOUNT) AS revenue
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY booking_month
ORDER BY booking_month;

-- 7. Avg stay duration by room type
SELECT
    ROOM_TYPE,
    ROUND(AVG(DATEDIFF('DAY', CHECK_IN_DATE, CHECK_OUT_DATE)), 1) AS avg_stay_days,
    MIN(DATEDIFF('DAY', CHECK_IN_DATE, CHECK_OUT_DATE)) AS min_stay,
    MAX(DATEDIFF('DAY', CHECK_IN_DATE, CHECK_OUT_DATE)) AS max_stay
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY ROOM_TYPE
ORDER BY avg_stay_days DESC;

-- 8. Avg revenue per booking by room type & currency
SELECT
    ROOM_TYPE,
    CURRENCY,
    COUNT(*) AS bookings,
    ROUND(AVG(TOTAL_AMOUNT), 2) AS avg_amount
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY ROOM_TYPE, CURRENCY
ORDER BY ROOM_TYPE, CURRENCY;

-- ============================================================
-- LEVEL 3: ADVANCED (Segmentation & Patterns)
-- ============================================================

-- 9. Cancellation & No-Show rate by city (top 10)
SELECT
    HOTEL_CITY,
    COUNT(*) AS total_bookings,
    ROUND(SUM(CASE WHEN BOOKING_STATUS = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_rate,
    ROUND(SUM(CASE WHEN BOOKING_STATUS = 'No-Show' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS noshow_rate
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY HOTEL_CITY
HAVING COUNT(*) >= 5
ORDER BY cancel_rate DESC
LIMIT 10;

-- 10. Cancellation rate by month
SELECT
    DATE_TRUNC('MONTH', CHECK_IN_DATE) AS booking_month,
    COUNT(*) AS total,
    ROUND(SUM(CASE WHEN BOOKING_STATUS = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_rate,
    ROUND(SUM(CASE WHEN BOOKING_STATUS = 'No-Show' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS noshow_rate
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY booking_month
ORDER BY booking_month;

-- 11. Revenue per guest by room type
SELECT
    ROOM_TYPE,
    ROUND(SUM(TOTAL_AMOUNT) / SUM(NUM_GUESTS), 2) AS revenue_per_guest,
    SUM(NUM_GUESTS) AS total_guests
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY ROOM_TYPE
ORDER BY revenue_per_guest DESC;

-- 12. Repeat customers
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    COUNT(*) AS booking_count,
    SUM(TOTAL_AMOUNT) AS total_spent
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY CUSTOMER_ID, CUSTOMER_NAME
HAVING COUNT(*) > 1
ORDER BY booking_count DESC
LIMIT 10;

-- ============================================================
-- LEVEL 4: DIFFICULT (Window Functions & Deep Analysis)
-- ============================================================

-- 13. Month-over-month revenue growth (%)
SELECT
    booking_month,
    revenue,
    LAG(revenue) OVER (ORDER BY booking_month) AS prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY booking_month)) * 100.0
        / NULLIF(LAG(revenue) OVER (ORDER BY booking_month), 0), 2) AS mom_growth_pct
FROM (
    SELECT
        DATE_TRUNC('MONTH', CHECK_IN_DATE) AS booking_month,
        SUM(TOTAL_AMOUNT) AS revenue
    FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
    GROUP BY booking_month
)
ORDER BY booking_month;

-- 14. Running total revenue over time
SELECT
    DATE_TRUNC('MONTH', CHECK_IN_DATE) AS booking_month,
    SUM(TOTAL_AMOUNT) AS monthly_revenue,
    SUM(SUM(TOTAL_AMOUNT)) OVER (ORDER BY DATE_TRUNC('MONTH', CHECK_IN_DATE)) AS cumulative_revenue
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY booking_month
ORDER BY booking_month;

-- 15. Rank cities by cancellation rate vs booking volume
SELECT
    HOTEL_CITY,
    COUNT(*) AS bookings,
    ROUND(SUM(CASE WHEN BOOKING_STATUS = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_rate,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS volume_rank,
    RANK() OVER (ORDER BY SUM(CASE WHEN BOOKING_STATUS = 'Cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) DESC) AS cancel_risk_rank
FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
GROUP BY HOTEL_CITY
HAVING COUNT(*) >= 3
ORDER BY cancel_risk_rank
LIMIT 15;

-- 16. Customer booking frequency distribution
SELECT
    CASE
        WHEN booking_count = 1 THEN '1 booking'
        WHEN booking_count = 2 THEN '2 bookings'
        WHEN booking_count BETWEEN 3 AND 5 THEN '3-5 bookings'
        ELSE '6+ bookings'
    END AS frequency_bucket,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS bucket_revenue
FROM (
    SELECT
        CUSTOMER_ID,
        COUNT(*) AS booking_count,
        SUM(TOTAL_AMOUNT) AS total_spent
    FROM SASWATDB.PUBLIC.HOTEL_BOOKING_CLEANED
    GROUP BY CUSTOMER_ID
)
GROUP BY frequency_bucket
ORDER BY customer_count DESC;

select sum(total_amount) as total from hotel_booking_cleaned

select  * from hotel_booking_cleaned