import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import date
import plotly.express as px

# --- Set credentials ---
import tempfile
import json

service_account_info = dict(st.secrets["gcp_service_account"])
with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as f:
    json.dump(service_account_info, f)
    f.flush()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name

st.set_page_config(page_title="Time to Hire & CAC", layout="wide")

# --- Load Main Data ---
@st.cache_data
def load_main_data():
    client = bigquery.Client()
    query = """
        SELECT 
            User_ID,
            `Application Created` AS application_date,
            `Successful_Date` AS successful_date,
            `Location Category Updated` AS location_category,
            `Nationality Category Updated` AS nationality_category,
            `Country Updated` AS country
        FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
        WHERE `Application Created` IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    df['application_date'] = pd.to_datetime(df['application_date'], errors='coerce')
    df['successful_date'] = pd.to_datetime(df['successful_date'], errors='coerce')
    return df.dropna(subset=['application_date'])

df = load_main_data()
df['app_month'] = df['application_date'].dt.to_period("M").dt.to_timestamp()
df['app_month_date'] = df['app_month'].dt.date
df['month_name'] = df['application_date'].dt.strftime("%b")
df['year'] = df['application_date'].dt.year

# --- Sidebar filters ---
st.sidebar.header("Filters")
nationality = st.sidebar.selectbox("Nationality Category", sorted(df['nationality_category'].dropna().unique()))
location = st.sidebar.selectbox("Location Category", sorted(df['location_category'].dropna().unique()))

# Show month slider before country
min_month = df['app_month_date'].min()
max_month = df['app_month_date'].max()
selected_month_range = st.sidebar.slider("Application Month Range", min_value=min_month, max_value=max_month, value=(min_month, max_month), format="MMM YYYY")
num_months = st.sidebar.slider("Months to Track After Application", min_value=1, max_value=12, value=6)

available_countries = sorted(df[(df['nationality_category'] == nationality) & (df['location_category'] == location)]['country'].dropna().unique())
countries = st.sidebar.multiselect("Country", available_countries, default=available_countries)

filtered_df = df[
    (df['nationality_category'] == nationality) &
    (df['location_category'] == location) &
    (df['country'].isin(countries)) &
    (df['app_month_date'] >= selected_month_range[0]) &
    (df['app_month_date'] <= selected_month_range[1])
]

# --- Compute Time to Hire ---
def compute_time_to_hire(df, num_months=12):
    df = df[df['successful_date'].notna()].copy()
    df['application_month'] = df['application_date'].dt.to_period("M").dt.to_timestamp()
    df['hire_month'] = df['successful_date'].dt.to_period("M").dt.to_timestamp()
    total_hires = len(df)
    bracket_counts = [0] * num_months
    for cohort_month, group in df.groupby('application_month'):
        for offset in range(num_months):
            start = cohort_month + pd.DateOffset(months=offset)
            end = cohort_month + pd.DateOffset(months=offset + 1)
            count = group[(group['hire_month'] >= start) & (group['hire_month'] < end)].shape[0]
            bracket_counts[offset] += count
    return [(c / total_hires if total_hires > 0 else 0) for c in bracket_counts]

# --- Month-wise Brackets ---
month_lookup = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
month_wise_brackets = {}
for month in month_lookup:
    month_data = filtered_df[filtered_df['month_name'] == month]
    month_wise_brackets[month] = compute_time_to_hire(month_data, 12) if not month_data.empty else [0.0] * 12

# --- Load Spend Data ---
@st.cache_data
def load_spend_data():
    client = bigquery.Client()
    query = """
        SELECT
            DATE_TRUNC(application_created_date, MONTH) AS spend_month,
            country_name,
            nationality_category,
            location_category,
            SUM(total_spend_aed) AS monthly_spend
        FROM `data-driven-attributes.AT_marketing_db.AT_Country_Daily_Performance_Spend_ERP_Updated`
        GROUP BY spend_month, country_name, nationality_category, location_category
    """
    return client.query(query).to_dataframe()

spend_df = load_spend_data()
spend_df['spend_month'] = pd.to_datetime(spend_df['spend_month']).dt.to_period('M').dt.to_timestamp()
spend_df = spend_df[
    (spend_df['nationality_category'] == nationality) &
    (spend_df['location_category'] == location) &
    (spend_df['country_name'].isin(countries))
]
spend_df = spend_df.groupby('spend_month', as_index=False)['monthly_spend'].sum()

# --- Load Hire Data ---
@st.cache_data
def load_hire_data():
    client = bigquery.Client()
    query = """
        SELECT
          DATE_TRUNC(DATE(Successful_Date), MONTH) AS hire_month,
          `Country Updated` AS country,
          `Location Category Updated` AS location_category,
          `Nationality Category Updated` AS nationality_category,
          COUNT(*) AS hires
        FROM `data-driven-attributes.AT_marketing_db.ATD_New_Last_Action_by_User_PivotData_View`
        WHERE Successful_Date IS NOT NULL
        GROUP BY hire_month, country, location_category, nationality_category
    """
    return client.query(query).to_dataframe()

hire_df = load_hire_data()
hire_df = hire_df[
    (hire_df['country'].isin(countries)) &
    (hire_df['location_category'] == location) &
    (hire_df['nationality_category'] == nationality)
]
hire_df['hire_month'] = pd.to_datetime(hire_df['hire_month']).dt.to_period("M").dt.to_timestamp()

# --- TABS ---
tab1, tab2, tab3, tab4 = st.tabs(["Overall Summary", "Monthly Drilldown", "Spend Overview", "Cost Calculator"])

with tab1:
    st.title("Overall Summary")
    summary = pd.DataFrame({
        'Time Bracket': [f"End of {i+1} Month{'s' if i > 0 else ''}" for i in range(num_months)],
        'Percentage of Total Hires': [f"{p * 100:.1f}%" for p in compute_time_to_hire(filtered_df, num_months)]
    })
    st.dataframe(summary, use_container_width=True)

    box_df = filtered_df[filtered_df['successful_date'].notna()].copy()
    box_df['time_to_hire_days'] = (box_df['successful_date'] - box_df['application_date']).dt.days
    for y in sorted(box_df['year'].unique()):
        fig = px.box(box_df[box_df['year'] == y], x='month_name', y='time_to_hire_days', title=f"Time to Hire by Month — {y}")
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.title("Monthly Drilldown")
    for m in month_lookup:
        st.subheader(f"{m}")
        percentages = month_wise_brackets.get(m, [0.0] * 12)[:num_months]
        df_m = pd.DataFrame({
            'Time Bracket': [f"End of {i+1} Month{'s' if i > 0 else ''}" for i in range(num_months)],
            'Percentage of Total Hires': [f"{p * 100:.1f}%" for p in percentages]
        })
        st.dataframe(df_m, use_container_width=True)

with tab3:
    st.title("Spend Overview")
    st.dataframe(spend_df.rename(columns={"spend_month": "Month"}), use_container_width=True)

with tab4:
    st.title("Cost Calculator")
    cac_rows = []

    for hire_month in sorted(hire_df['hire_month'].dropna().unique()):
        hires = int(hire_df[hire_df['hire_month'] == hire_month]['hires'].sum())
        weighted_spend = 0
        for i in range(12):
            spend_month = (hire_month - pd.DateOffset(months=i)).to_period('M').to_timestamp()
            spend = spend_df[spend_df['spend_month'] == spend_month]['monthly_spend'].sum()
            month_name = spend_month.strftime("%b")
            weight = month_wise_brackets.get(month_name, [0]*12)[i]
            weighted_spend += spend * weight
        cac = (weighted_spend / hires) if hires > 0 else None
        cac_rows.append({
            'Hire Month': hire_month.strftime('%Y-%m'),
            'Total Hires': hires,
            'Weighted Spend (AED)': round(weighted_spend, 2),
            'CAC (AED per Hire)': round(cac, 2) if cac is not None else 'N/A'
        })

    cac_df = pd.DataFrame(cac_rows)
    st.dataframe(cac_df, use_container_width=True)
    st.download_button("Download CAC Results", data=cac_df.to_csv(index=False), file_name="cac_results.csv", mime="text/csv")
