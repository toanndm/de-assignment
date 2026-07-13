-- =============================================================
-- queries.sql
-- Two analytical queries against the fact_events schema
-- Target: PostgreSQL 15
-- =============================================================


-- -------------------------------------------------------------
-- QUERY 1: Monthly revenue by event type
--
-- What it answers:
--   How does revenue (total_value) trend month-over-month,
--   broken down by event type?
--   Useful for spotting seasonal patterns and which service
--   tier drives the most revenue.
--
-- Why it's worth asking:
--   Revenue trends per product category are a fundamental KPI
--   for any ride/delivery platform. A drop in 'premium' revenue
--   in a specific month could indicate pricing or supply issues.
-- -------------------------------------------------------------

SELECT
    DATE_TRUNC('month', fe.event_timestamp)::DATE  AS month,
    et.name                                         AS event_type,
    COUNT(*)                                        AS total_events,
    ROUND(SUM(fe.total_value),        2)            AS gross_revenue,
    ROUND(AVG(fe.total_value),        2)            AS avg_revenue_per_event,
    ROUND(SUM(fe.sub_value),          2)            AS total_surcharge,
    COUNT(*) FILTER (WHERE fe.is_anomaly)           AS anomaly_count
FROM  fact_events fe
JOIN  dim_event_type et ON et.id = fe.event_type_id
WHERE fe.is_anomaly = FALSE          -- exclude negative-value anomalies
GROUP BY 1, 2
ORDER BY 1, gross_revenue DESC;


-- -------------------------------------------------------------
-- QUERY 2: Top entities by revenue with payment method breakdown
--
-- What it answers:
--   Which entity_ids generate the most revenue, and what
--   payment methods do their customers prefer?
--   Also surfaces the average trip duration for each top entity.
--
-- Why it's worth asking:
--   Identifying high-value entities (drivers/vehicles) helps
--   with loyalty programs and incentive targeting.
--   Payment method preference per entity can inform cashless
--   adoption initiatives.
-- -------------------------------------------------------------

WITH entity_totals AS (
    SELECT
        fe.entity_id,
        COUNT(*)                                AS total_trips,
        ROUND(SUM(fe.total_value),   2)         AS total_revenue,
        ROUND(AVG(fe.total_value),   2)         AS avg_revenue,
        ROUND(AVG(fe.duration_seconds) / 60.0, 1) AS avg_duration_min,
        -- payment method counts
        COUNT(*) FILTER (WHERE pm.name = 'card')    AS paid_card,
        COUNT(*) FILTER (WHERE pm.name = 'cash')    AS paid_cash,
        COUNT(*) FILTER (WHERE pm.name = 'account') AS paid_account,
        COUNT(*) FILTER (WHERE pm.name = 'voucher') AS paid_voucher
    FROM  fact_events fe
    JOIN  dim_payment_method pm ON pm.id = fe.payment_method_id
    WHERE fe.is_anomaly = FALSE
    GROUP BY fe.entity_id
)
SELECT
    entity_id,
    total_trips,
    total_revenue,
    avg_revenue,
    avg_duration_min,
    paid_card,
    paid_cash,
    paid_account,
    paid_voucher,
    -- dominant payment method
    CASE GREATEST(paid_card, paid_cash, paid_account, paid_voucher)
        WHEN paid_card    THEN 'card'
        WHEN paid_cash    THEN 'cash'
        WHEN paid_account THEN 'account'
        ELSE                   'voucher'
    END AS dominant_payment
FROM  entity_totals
ORDER BY total_revenue DESC
LIMIT 20;
