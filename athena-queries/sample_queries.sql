-- ============================================================================
-- COMPREHENSIVE ATHENA QUERIES FOR PAYMENT TRANSACTION DATA
-- ============================================================================
-- These queries analyze authorization, clearing, and chargeback data
-- Generated data includes realistic business logic for payment processing
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DAILY TRANSACTION VOLUME AND REVENUE ANALYSIS
-- ----------------------------------------------------------------------------
-- Analyze daily transaction patterns, approval rates, and revenue
SELECT 
    DATE(CAST(process_date AS TIMESTAMP)) as transaction_date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) as approved_count,
    SUM(CASE WHEN transaction_status_code = 5 THEN 1 ELSE 0 END) as declined_count,
    ROUND(100.0 * SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_rate_pct,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_amount_usd,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_transaction_usd,
    ROUND(STDDEV(transaction_amount_cents) / 100.0, 2) as stddev_amount_usd
FROM payment_data.authorization
WHERE year = '2024' AND month = '01'
GROUP BY DATE(CAST(process_date AS TIMESTAMP))
ORDER BY transaction_date;

-- ----------------------------------------------------------------------------
-- 2. TRANSACTION TYPE DISTRIBUTION AND PERFORMANCE
-- ----------------------------------------------------------------------------
-- Analyze different transaction types (PURCHASE, CASH_ADVANCE, etc.)
SELECT 
    transaction_type,
    transaction_type_cd,
    COUNT(*) as transaction_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total,
    SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) as approved,
    SUM(CASE WHEN transaction_status_code = 5 THEN 1 ELSE 0 END) as declined,
    ROUND(100.0 * SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_rate_pct,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_revenue_usd,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_amount_usd
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY transaction_type, transaction_type_cd
ORDER BY transaction_count DESC;

-- ----------------------------------------------------------------------------
-- 3. CARD BRAND AND PRODUCT PERFORMANCE
-- ----------------------------------------------------------------------------
-- Compare performance across card brands and products
SELECT 
    card_brand,
    card_product_id,
    COUNT(*) as transactions,
    ROUND(100.0 * SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_rate_pct,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_volume_usd,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_ticket_usd,
    ROUND(AVG(CAST(issuer_rate AS DOUBLE)) * 100, 4) as avg_issuer_rate_pct,
    ROUND(AVG(CAST(interchange_rate AS DOUBLE)) * 100, 4) as avg_interchange_rate_pct
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY card_brand, card_product_id
ORDER BY transactions DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 4. CROSS-BORDER TRANSACTION ANALYSIS
-- ----------------------------------------------------------------------------
-- Identify cross-border transactions and analyze exchange rates
SELECT 
    issuer_country_code as card_country,
    merchant_country_code,
    COUNT(*) as transactions,
    ROUND(AVG(CAST(exchange_rate AS DOUBLE)), 4) as avg_exchange_rate,
    ROUND(MIN(CAST(exchange_rate AS DOUBLE)), 4) as min_exchange_rate,
    ROUND(MAX(CAST(exchange_rate AS DOUBLE)), 4) as max_exchange_rate,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_volume_usd,
    ROUND(100.0 * SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_rate_pct
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND issuer_country_code != merchant_country_code
GROUP BY issuer_country_code, merchant_country_code
HAVING COUNT(*) > 10
ORDER BY transactions DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 5. PAYMENT PROCESSING RATE ANALYSIS
-- ----------------------------------------------------------------------------
-- Analyze all processing rates (issuer, network, acquirer, interchange, processing)
SELECT 
    transaction_type,
    COUNT(*) as transactions,
    ROUND(AVG(CAST(issuer_rate AS DOUBLE)) * 100, 4) as avg_issuer_rate_pct,
    ROUND(AVG(CAST(network_rate AS DOUBLE)) * 100, 4) as avg_network_rate_pct,
    ROUND(AVG(CAST(acquirer_rate AS DOUBLE)) * 100, 4) as avg_acquirer_rate_pct,
    ROUND(AVG(CAST(interchange_rate AS DOUBLE)) * 100, 4) as avg_interchange_rate_pct,
    ROUND(AVG(CAST(processing_rate AS DOUBLE)) * 100, 4) as avg_processing_rate_pct,
    ROUND(AVG(CAST(risk_rate AS DOUBLE)) * 100, 4) as avg_risk_rate_pct,
    ROUND(AVG(CAST(issuer_rate AS DOUBLE) + CAST(network_rate AS DOUBLE) + 
              CAST(acquirer_rate AS DOUBLE) + CAST(interchange_rate AS DOUBLE) + 
              CAST(processing_rate AS DOUBLE)) * 100, 4) as total_avg_rate_pct
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND transaction_status_code = 0
GROUP BY transaction_type
ORDER BY transactions DESC;

-- ----------------------------------------------------------------------------
-- 6. CHARGEBACK ANALYSIS
-- ----------------------------------------------------------------------------
-- Analyze chargeback patterns and reasons
SELECT 
    cb.chargeback_reason_code,
    COUNT(*) as chargeback_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_chargebacks,
    ROUND(SUM(cb.chargeback_amount_cents) / 100.0, 2) as total_chargeback_amount_usd,
    ROUND(AVG(cb.chargeback_amount_cents) / 100.0, 2) as avg_chargeback_amount_usd,
    COUNT(DISTINCT a.card_brand) as affected_card_brands,
    COUNT(DISTINCT a.merchant_category_code) as affected_mcc_codes
FROM payment_data.chargeback cb
JOIN payment_data.authorization a 
    ON cb.hash_pan = a.hash_pan 
    AND cb.sequence_number = a.sequence_number
WHERE cb.year = '2024' AND cb.month = '01' AND cb.day = '15'
GROUP BY cb.chargeback_reason_code
ORDER BY chargeback_count DESC;

-- ----------------------------------------------------------------------------
-- 7. REFUND AND REVERSAL ANALYSIS
-- ----------------------------------------------------------------------------
-- Analyze refund and reversal patterns
SELECT 
    transaction_type,
    transaction_status_code,
    CASE 
        WHEN transaction_status_code = 3 THEN 'Reversed'
        WHEN transaction_status_code = 4 THEN 'Refunded'
        WHEN transaction_status_code = 0 THEN 'Approved'
        WHEN transaction_status_code = 5 THEN 'Declined'
        WHEN transaction_status_code = 6 THEN 'Chargeback'
        ELSE 'Other'
    END as status_description,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN refund_amount_cents > 0 THEN 1 ELSE 0 END) as has_refund_count,
    SUM(CASE WHEN reversal_amount_cents > 0 THEN 1 ELSE 0 END) as has_reversal_count,
    ROUND(SUM(refund_amount_cents) / 100.0, 2) as total_refund_amount_usd,
    ROUND(SUM(reversal_amount_cents) / 100.0, 2) as total_reversal_amount_usd
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY transaction_type, transaction_status_code
ORDER BY transaction_count DESC;

-- ----------------------------------------------------------------------------
-- 8. SETTLEMENT STATUS ANALYSIS (CLEARING TABLE)
-- ----------------------------------------------------------------------------
-- Analyze settlement patterns from clearing data
SELECT 
    settlement_status,
    clearing_status,
    COUNT(*) as transaction_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_amount_usd,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_amount_usd,
    COUNT(DISTINCT merchant_id) as unique_merchants,
    COUNT(DISTINCT issuer_id) as unique_issuers
FROM payment_data.clearing
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY settlement_status, clearing_status
ORDER BY transaction_count DESC;

-- ----------------------------------------------------------------------------
-- 9. MERCHANT CATEGORY CODE (MCC) ANALYSIS
-- ----------------------------------------------------------------------------
-- Analyze transaction patterns by merchant category
SELECT 
    merchant_category_code,
    COUNT(*) as transactions,
    ROUND(100.0 * SUM(CASE WHEN transaction_status_code = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_rate_pct,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_volume_usd,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_ticket_usd,
    SUM(CASE WHEN tip_amount_cents > 0 THEN 1 ELSE 0 END) as transactions_with_tips,
    ROUND(SUM(tip_amount_cents) / 100.0, 2) as total_tips_usd,
    SUM(CASE WHEN shipping_amount_cents > 0 THEN 1 ELSE 0 END) as transactions_with_shipping
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY merchant_category_code
HAVING COUNT(*) > 50
ORDER BY total_volume_usd DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 10. BILLING ADDRESS COUNTRY CORRELATION
-- ----------------------------------------------------------------------------
-- Verify billing address country matches cardholder country
SELECT 
    c.cardholder_country,
    c.bill_addr_country,
    COUNT(*) as transaction_count,
    CASE 
        WHEN c.cardholder_country = c.bill_addr_country THEN 'MATCH'
        ELSE 'MISMATCH'
    END as country_match_status,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total
FROM payment_data.clearing c
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY c.cardholder_country, c.bill_addr_country
HAVING COUNT(*) > 10
ORDER BY transaction_count DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 11. TRANSACTION LIFECYCLE JOIN (AUTH -> CLEARING -> CHARGEBACK)
-- ----------------------------------------------------------------------------
-- Join all three tables to see complete transaction lifecycle
SELECT 
    a.sequence_number,
    a.hash_pan,
    a.transaction_type,
    a.transaction_status_code,
    ROUND(a.transaction_amount_cents / 100.0, 2) as auth_amount_usd,
    c.settlement_status,
    c.clearing_status,
    ROUND(c.transaction_amount_cents / 100.0, 2) as clearing_amount_usd,
    cb.chargeback_reason_code,
    ROUND(cb.chargeback_amount_cents / 100.0, 2) as chargeback_amount_usd,
    CASE 
        WHEN cb.sequence_number IS NOT NULL THEN 'Has Chargeback'
        WHEN a.transaction_status_code = 4 THEN 'Refunded'
        WHEN a.transaction_status_code = 3 THEN 'Reversed'
        WHEN a.transaction_status_code = 0 THEN 'Completed'
        ELSE 'Other'
    END as final_status
FROM payment_data.authorization a
LEFT JOIN payment_data.clearing c 
    ON a.hash_pan = c.hash_pan 
    AND a.sequence_number = c.sequence_number
LEFT JOIN payment_data.chargeback cb 
    ON a.hash_pan = cb.hash_pan 
    AND a.sequence_number = cb.sequence_number
WHERE a.year = '2024' AND a.month = '01' AND a.day = '15'
LIMIT 100;

-- ----------------------------------------------------------------------------
-- 12. CURRENCY ANALYSIS
-- ----------------------------------------------------------------------------
-- Analyze transactions by currency with exchange rates
SELECT 
    currency_code,
    issuer_country_code,
    COUNT(*) as transactions,
    ROUND(AVG(CAST(exchange_rate AS DOUBLE)), 4) as avg_exchange_rate,
    ROUND(SUM(transaction_amount_cents) / 100.0, 2) as total_volume_usd,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_amount_usd,
    COUNT(DISTINCT merchant_id) as unique_merchants
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY currency_code, issuer_country_code
HAVING COUNT(*) > 20
ORDER BY transactions DESC;

-- ----------------------------------------------------------------------------
-- 13. HASH TABLE VALIDATION
-- ----------------------------------------------------------------------------
-- Verify hash tables match main tables
SELECT 
    'authorization' as table_name,
    COUNT(DISTINCT a.sequence_number) as main_table_sequences,
    COUNT(DISTINCT ah.sequence_number) as hash_table_sequences,
    CASE 
        WHEN COUNT(DISTINCT a.sequence_number) = COUNT(DISTINCT ah.sequence_number) 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as validation_status
FROM payment_data.authorization a
FULL OUTER JOIN payment_data.authorization_hash ah 
    ON a.sequence_number = ah.sequence_number
WHERE a.year = '2024' AND a.month = '01' AND a.day = '15'
UNION ALL
SELECT 
    'clearing' as table_name,
    COUNT(DISTINCT c.sequence_number) as main_table_sequences,
    COUNT(DISTINCT ch.sequence_number) as hash_table_sequences,
    CASE 
        WHEN COUNT(DISTINCT c.sequence_number) = COUNT(DISTINCT ch.sequence_number) 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as validation_status
FROM payment_data.clearing c
FULL OUTER JOIN payment_data.clearing_hash ch 
    ON c.sequence_number = ch.sequence_number
WHERE c.year = '2024' AND c.month = '01' AND c.day = '15';

-- ----------------------------------------------------------------------------
-- 14. DAILY CHARGEBACK RATE CALCULATION
-- ----------------------------------------------------------------------------
-- Calculate chargeback rate as percentage of total transactions
SELECT 
    DATE(CAST(a.process_date AS TIMESTAMP)) as transaction_date,
    COUNT(DISTINCT a.sequence_number) as total_transactions,
    COUNT(DISTINCT cb.sequence_number) as chargeback_transactions,
    ROUND(100.0 * COUNT(DISTINCT cb.sequence_number) / COUNT(DISTINCT a.sequence_number), 4) as chargeback_rate_pct,
    ROUND(SUM(a.transaction_amount_cents) / 100.0, 2) as total_volume_usd,
    ROUND(SUM(cb.chargeback_amount_cents) / 100.0, 2) as chargeback_volume_usd,
    ROUND(100.0 * SUM(cb.chargeback_amount_cents) / SUM(a.transaction_amount_cents), 4) as chargeback_amount_rate_pct
FROM payment_data.authorization a
LEFT JOIN payment_data.chargeback cb 
    ON a.hash_pan = cb.hash_pan 
    AND a.sequence_number = cb.sequence_number
WHERE a.year = '2024' AND a.month = '01'
GROUP BY DATE(CAST(a.process_date AS TIMESTAMP))
ORDER BY transaction_date;

-- ----------------------------------------------------------------------------
-- 15. TIP RATE VALIDATION (Should be ~40% for approved non-refund transactions)
-- ----------------------------------------------------------------------------
SELECT 
    transaction_type,
    COUNT(*) as total_approved_non_refund,
    SUM(CASE WHEN tip_amount_cents > 0 THEN 1 ELSE 0 END) as txns_with_tips,
    ROUND(100.0 * SUM(CASE WHEN tip_amount_cents > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as tip_rate_pct,
    ROUND(SUM(tip_amount_cents) / 100.0, 2) as total_tips_usd,
    ROUND(AVG(CASE WHEN tip_amount_cents > 0 THEN tip_amount_cents END) / 100.0, 2) as avg_tip_when_present
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND transaction_status_code = 0
  AND transaction_status_code != 4
GROUP BY transaction_type
ORDER BY total_approved_non_refund DESC;

-- ----------------------------------------------------------------------------
-- 16. SHIPPING AND HANDLING CORRELATION (50% of shipped items should have handling)
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN shipping_amount_cents > 0 AND handling_amount_cents > 0 THEN 'Both Shipping & Handling'
        WHEN shipping_amount_cents > 0 THEN 'Shipping Only'
        ELSE 'No Shipping'
    END as shipping_category,
    COUNT(*) as txn_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total,
    ROUND(SUM(shipping_amount_cents) / 100.0, 2) as total_shipping_usd,
    ROUND(SUM(handling_amount_cents) / 100.0, 2) as total_handling_usd
FROM payment_data.clearing
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY 
    CASE 
        WHEN shipping_amount_cents > 0 AND handling_amount_cents > 0 THEN 'Both Shipping & Handling'
        WHEN shipping_amount_cents > 0 THEN 'Shipping Only'
        ELSE 'No Shipping'
    END
ORDER BY txn_count DESC;

-- ----------------------------------------------------------------------------
-- 17. RATE VARIATIONS BY TRANSACTION TYPE (CASH_ADVANCE highest, PURCHASE lowest)
-- ----------------------------------------------------------------------------
SELECT 
    transaction_type,
    COUNT(*) as txn_count,
    ROUND(AVG(CAST(issuer_rate AS DOUBLE)) * 100, 4) as avg_issuer_rate_pct,
    ROUND(MIN(CAST(issuer_rate AS DOUBLE)) * 100, 4) as min_issuer_rate_pct,
    ROUND(MAX(CAST(issuer_rate AS DOUBLE)) * 100, 4) as max_issuer_rate_pct,
    ROUND(AVG(CAST(acquirer_rate AS DOUBLE)) * 100, 4) as avg_acquirer_rate_pct,
    ROUND(AVG(CAST(interchange_rate AS DOUBLE)) * 100, 4) as avg_interchange_rate_pct
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND transaction_status_code = 0
GROUP BY transaction_type
ORDER BY avg_issuer_rate_pct DESC;

-- ----------------------------------------------------------------------------
-- 18. CROSS-BORDER RATE MARKUP ANALYSIS (Network rates 50-150% higher, Interchange 20-50% higher)
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN issuer_country_code = merchant_country_code THEN 'Domestic'
        ELSE 'Cross-Border'
    END as transaction_category,
    COUNT(*) as txn_count,
    ROUND(AVG(CAST(network_rate AS DOUBLE)) * 100, 4) as avg_network_rate_pct,
    ROUND(AVG(CAST(interchange_rate AS DOUBLE)) * 100, 4) as avg_interchange_rate_pct,
    ROUND(AVG(CAST(exchange_rate AS DOUBLE)), 4) as avg_exchange_rate
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND transaction_status_code = 0
GROUP BY 
    CASE 
        WHEN issuer_country_code = merchant_country_code THEN 'Domestic'
        ELSE 'Cross-Border'
    END
ORDER BY transaction_category;

-- ----------------------------------------------------------------------------
-- 19. SMALL TRANSACTION ACQUIRER RATE MARKUP (10-30% higher for txns < $100)
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN transaction_amount_cents < 10000 THEN 'Under $100'
        WHEN transaction_amount_cents < 50000 THEN '$100-$500'
        WHEN transaction_amount_cents < 100000 THEN '$500-$1000'
        ELSE 'Over $1000'
    END as amount_bucket,
    COUNT(*) as txn_count,
    ROUND(AVG(CAST(acquirer_rate AS DOUBLE)) * 100, 4) as avg_acquirer_rate_pct,
    ROUND(MIN(CAST(acquirer_rate AS DOUBLE)) * 100, 4) as min_acquirer_rate_pct,
    ROUND(MAX(CAST(acquirer_rate AS DOUBLE)) * 100, 4) as max_acquirer_rate_pct,
    ROUND(AVG(transaction_amount_cents) / 100.0, 2) as avg_amount_usd
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND transaction_status_code = 0
GROUP BY 
    CASE 
        WHEN transaction_amount_cents < 10000 THEN 'Under $100'
        WHEN transaction_amount_cents < 50000 THEN '$100-$500'
        WHEN transaction_amount_cents < 100000 THEN '$500-$1000'
        ELSE 'Over $1000'
    END
ORDER BY 
    CASE 
        WHEN amount_bucket = 'Under $100' THEN 1
        WHEN amount_bucket = '$100-$500' THEN 2
        WHEN amount_bucket = '$500-$1000' THEN 3
        ELSE 4
    END;

-- ----------------------------------------------------------------------------
-- 20. RISK RATE ANALYSIS (Higher for declined, 1.2x multiplier for tips)
-- ----------------------------------------------------------------------------
SELECT 
    CASE WHEN transaction_status_code = 5 THEN 'Declined' ELSE 'Approved' END as txn_status,
    CASE WHEN tip_amount_cents > 0 THEN 'Has Tip' ELSE 'No Tip' END as tip_status,
    COUNT(*) as txn_count,
    ROUND(AVG(CAST(risk_rate AS DOUBLE)) * 100, 4) as avg_risk_rate_pct,
    ROUND(MIN(CAST(risk_rate AS DOUBLE)) * 100, 4) as min_risk_rate_pct,
    ROUND(MAX(CAST(risk_rate AS DOUBLE)) * 100, 4) as max_risk_rate_pct
FROM payment_data.authorization
WHERE year = '2024' AND month = '01' AND day = '15'
  AND transaction_status_code IN (0, 5)
GROUP BY 
    CASE WHEN transaction_status_code = 5 THEN 'Declined' ELSE 'Approved' END,
    CASE WHEN tip_amount_cents > 0 THEN 'Has Tip' ELSE 'No Tip' END
ORDER BY txn_status, tip_status;
