-- Filter transactions to only those from the last 12 months
WITH recent_transactions AS (
    SELECT *
    FROM Transactions
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '12 months'
),

-- Filter campaign interactions to only those from the last 12 months
recent_interactions AS (
    SELECT *
    FROM CampaignInteractions
    WHERE interaction_timestamp >= CURRENT_DATE - INTERVAL '12 months'
),

-- Attribute transactions to the most recent campaign interaction within 7 days before the transaction
interaction_attribution AS (
    SELECT
        rt.transaction_id,
        rt.customer_id,
        rt.transaction_date,
        rt.total_amount,
        ci.campaign_id,
        ci.interaction_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY rt.transaction_id 
            ORDER BY ci.interaction_timestamp DESC
        ) AS rn  -- rank interactions per transaction, latest = rn = 1
    FROM recent_transactions rt
    JOIN recent_interactions ci
        ON rt.customer_id = ci.customer_id
        AND ci.interaction_timestamp BETWEEN rt.transaction_date - INTERVAL '7 days' AND rt.transaction_date
),

-- Keep only the most recent (latest) interaction per transaction
attributed_transactions AS (
    SELECT *
    FROM interaction_attribution
    WHERE rn = 1
),

-- Identify all unique customers who interacted with each campaign
interacted_customers AS (
    SELECT DISTINCT customer_id, campaign_id
    FROM recent_interactions
),

-- Aggregate metrics for each campaign
aggregated_metrics AS (
    SELECT
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.budget AS total_budget,
        
        -- How many unique customers interacted with the campaign
        COUNT(DISTINCT ic.customer_id) AS unique_interacted_customers,

        -- Total number of all interactions with the campaign
        COUNT(ri.interaction_id) AS total_interaction,

        -- Number of transactions attributed to the campaign
        COUNT(DISTINCT at.transaction_id) AS attributed_transactions_count,

        -- Total revenue from those attributed transactions
        COALESCE(SUM(at.total_amount), 0) AS attributed_revenue,

        -- ROI = attributed revenue / budget
        ROUND(COALESCE(SUM(at.total_amount), 0) / NULLIF(mc.budget, 0), 2) AS roi
    FROM MarketingCampaigns mc
    LEFT JOIN interacted_customers ic ON mc.campaign_id = ic.campaign_id
    LEFT JOIN recent_interactions ri ON mc.campaign_id = ri.campaign_id
    LEFT JOIN attributed_transactions at ON mc.campaign_id = at.campaign_id
    GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.budget
)

-- Return top 5 campaigns by attributed revenue
SELECT *
FROM aggregated_metrics
ORDER BY attributed_revenue DESC
LIMIT 5;
