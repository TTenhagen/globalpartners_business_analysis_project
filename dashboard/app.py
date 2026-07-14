import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Global Partners CLV & Insights", layout="wide")

page = st.sidebar.selectbox("Dashboard", [
    "Customer Segmentation", "Churn Risk Indicators", "Sales Trends & Seasonality",
    "Loyalty Program Impact", "Location Performance", "Pricing & Discount Effectiveness"])

@st.cache_data(ttl=3600)
def load_gold(table_name: str) -> pd.DataFrame:
# Gold layer only -- never raw or Silver. Once the Redshift COPY loads
# (sql/02) are live, swap this to a Redshift query -- the only change needed.
    return pd.read_parquet(f"s3://restaruant-raw/gold/{table_name}/")

# 1 -- Customer Segmentation ..................... customer_rfm_segments
if page == "Customer Segmentation":
    df = load_gold("customer_rfm_segments")
    c1, c2, c3 = st.columns(3)
    c1.metric("Customers", f"{df['user_id'].nunique():,}")
    c2.metric("VIP", int((df["segment"] == "VIP").sum()))
    c3.metric("Churn Risk", int((df["segment"] == "Churn Risk").sum()))

fig = px.scatter(df, x="frequency", y="monetary", color="segment",
size="recency_days", hover_data=["user_id"])
st.plotly_chart(fig, use_container_width=True)
st.bar_chart(df["segment"].value_counts())

# 2 -- Churn Risk Indicators ..................... customer_churn_indicators
elif page == "Churn Risk Indicators":
df = load_gold("customer_churn_indicators")
at_risk = df[df["is_at_risk"]]
c1, c2, c3 = st.columns(3)
c1.metric("At-Risk Customers", f"{len(at_risk):,}",
delta=f"{len(at_risk)/len(df):.0%} of base", delta_color="inverse")
c2.metric("Avg Days Since Last Order", f"{df['days_since_last_order'].mean():.0f}")
c3.metric("Avg Inter-Order Gap", f"{df['avg_inter_order_gap_days'].mean():.1f} d")
fig = px.histogram(df, x="days_since_last_order", nbins=40)
fig.add_vline(x=45, line_color="red", line_dash="dash",
annotation_text="45-day at-risk threshold")
st.plotly_chart(fig, use_container_width=True)
# REQUIRED: visible threshold-based alert -- red highlight > 45 days
st.subheader("At-risk customers (>45 days inactive)")
st.dataframe(
at_risk.sort_values("days_since_last_order", ascending=False)
.style.map(lambda v: "background-color: #f8d7da",
subset=["days_since_last_order"]))

# 3 -- Sales Trends & Seasonality ................ sales_trends_daily
elif page == "Sales Trends & Seasonality":
df = load_gold("sales_trends_daily")
cats = st.sidebar.multiselect("Category",
sorted(df["item_category"].dropna().unique()))
if cats:
df = df[df["item_category"].isin(cats)]
daily = df.groupby("sales_date")["total_revenue"].sum().reset_index()
st.plotly_chart(px.line(daily, x="sales_date", y="total_revenue",
title="Daily Revenue"), use_container_width=True)
monthly = df.groupby(["year", "month"])["total_revenue"].sum().reset_index()
st.plotly_chart(px.bar(monthly, x="month", y="total_revenue", color="year",
barmode="group", title="Monthly / Seasonal Revenue"),
use_container_width=True)
wk = (df.groupby("is_weekend")["total_revenue"].mean().reset_index()
.replace({True: "Weekend", False: "Weekday"}))
st.plotly_chart(px.bar(wk, x="is_weekend", y="total_revenue",
title="Avg Daily Revenue: Weekend vs Weekday"),
use_container_width=True)

# 4 -- Loyalty Program Impact ................... loyalty_program_comparison
elif page == "Loyalty Program Impact":
df = load_gold("loyalty_program_comparison")
df["group"] = df["is_loyalty"].map({True: "Loyalty", False: "Non-Loyalty"})
c1, c2 = st.columns(2)
c1.plotly_chart(px.bar(df, x="group", y="avg_spend", title="Avg Spend"),
use_container_width=True)
c2.plotly_chart(px.bar(df, x="group", y="avg_clv", title="Avg CLV"),
use_container_width=True)
st.dataframe(df[["group", "avg_spend", "repeat_orders",
"avg_clv", "customer_count"]])

# 5 -- Location Performance ..................... location_performance
elif page == "Location Performance":
df = load_gold("location_performance").sort_values("revenue_rank")
c1, c2, c3 = st.columns(3)
c1.metric("Locations", len(df))
c2.metric("Top Location Revenue", f"${df['total_revenue'].max():,.0f}")
c3.metric("Median AOV", f"${df['avg_order_value'].median():.2f}")
st.plotly_chart(px.bar(df.head(15), x="restaurant_id", y="total_revenue",
title="Top 15 Locations by Revenue"),
use_container_width=True)
# The "why": is revenue driven by traffic (orders/day) or ticket size (AOV)?
st.plotly_chart(px.scatter(df, x="orders_per_day", y="avg_order_value",
size="total_revenue", hover_data=["restaurant_id"],
title="Traffic vs Ticket Size"),
use_container_width=True)

# 6 -- Pricing & Discount Effectiveness ......... discount_effectiveness
elif page == "Pricing & Discount Effectiveness":
df = load_gold("discount_effectiveness")
df["group"] = df["has_discount"].map({True: "Discounted", False: "Full Price"})
c1, c2 = st.columns(2)
c1.plotly_chart(px.bar(df, x="group", y="order_count",
title="Order Volume"), use_container_width=True)
c2.plotly_chart(px.bar(df, x="group", y="avg_order_revenue",
title="Avg Order Revenue"), use_container_width=True)
st.dataframe(df[["group", "order_count", "avg_order_revenue", "total_revenue"]])