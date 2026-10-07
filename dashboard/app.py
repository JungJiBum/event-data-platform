import os

import pandas as pd
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="Event Data Dashboard",
    layout="wide",
)

st.title("Event Data Dashboard")
st.write("Data Mart 기반 이벤트 분석 대시보드")


def load_daily_actor_stats():
    connection = psycopg2.connect(
        host="db",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    query = """
        SELECT
            stat_date,
            actor_id,
            actor_name,
            event_count
        FROM daily_actor_stats
        ORDER BY stat_date DESC, actor_id;
    """

    df = pd.read_sql(query, connection)
    connection.close()

    return df


df = load_daily_actor_stats()
total_events = int(df["event_count"].sum())
active_actors = int(df["actor_id"].nunique())

col1, col2 = st.columns(2)

with col1:
    st.metric("Total Events", f"{total_events:,}")

with col2:
    st.metric("Active Actors", active_actors)

st.subheader("Daily Actor Stats")
st.dataframe(df, use_container_width=True)
st.subheader("Events by Actor")

actor_stats = (
    df.groupby("actor_name", as_index=False)["event_count"]
    .sum()
    .set_index("actor_name")
)

st.bar_chart(actor_stats)

st.subheader("Daily Event Trend")

daily_stats = (
    df.groupby("stat_date", as_index=False)["event_count"]
    .sum()
    .set_index("stat_date")
)

st.line_chart(daily_stats)