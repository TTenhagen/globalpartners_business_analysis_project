--daily CLV snapshot
CREATE TABLE IF NOT EXISTS customer_clv_daily (
    user_id VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    daily_spend DECIMAL(12,2),
    cumulative_spend DECIMAL(12,2),
    order_count_to_date INTEGER,
    avg_order_value DECIMAL(10,2),
    clv_to_date DECIMAL(12,2),
    clv_tier VARCHAR(10),
    days_since_first_order INTEGER,
    insert_date TIMESTAMP,
    PRIMARY KEY (user_id, snapshot_date)
    )
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (snapshot_date);

-- RFM segmentation
CREATE TABLE IF NOT EXISTS customer_rfm_segments (
    user_id VARCHAR(64) NOT NULL,
    recency_days INTEGER,
    frequency INTEGER,
    monetary DECIMAL(12,2),
    r_score SMALLINT,
    f_score SMALLINT,
    m_score SMALLINT,
    segment VARCHAR(20),
    last_order_date DATE,
    PRIMARY KEY (user_id)
    )
DISTSTYLE KEY
DISTKEY (user_id);

-- Churn risk indicators
CREATE TABLE IF NOT EXISTS customer_churn_indicators (
    user_id VARCHAR(64) NOT NULL,
    days_since_last_order INTEGER,
    avg_inter_order_gap_days DECIMAL(8,2),
    spend_change_pct DECIMAL(8,2),
    s_at_risk BOOLEAN,
    PRIMARY KEY (user_id)
);

-- Sales trends
CREATE TABLE IF NOT EXISTS sales_trends_daily (
    sales_date DATE NOT NULL,
    restaurant_id VARCHAR(64),
    item_category VARCHAR(64),
    total_revenue DECIMAL(12,2),
    order_count INTEGER,
    PRIMARY KEY (sales_date, restaurant_id, item_category)
)
SORTKEY (sales_date)
;

-- Loyalty program comparison
CREATE TABLE IF NOT EXISTS loyalty_program_comparison (
    is_loyalty BOOLEAN NOT NULL,
    avg_spend DECIMAL(10,2),
    repeat_orders INTEGER,
    avg_clv DECIMAL(12,2),
    customer_count INTEGER,
    PRIMARY KEY (is_loyalty)
);

-- Location performance
CREATE TABLE IF NOT EXISTS location_performance (
    restaurant_id VARCHAR(64) NOT NULL,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    orders_per_day DECIMAL(10,2),
    PRIMARY KEY (restaurant_id)
);

-- Discount effectiveness
CREATE TABLE IF NOT EXISTS discount_effectiveness (
    has_discount BOOLEAN NOT NULL,
    order_count INTEGER,
    avg_order_revenue DECIMAL(10,2),
    total_revenue DECIMAL(14,2),
    PRIMARY KEY (has_discount)
);