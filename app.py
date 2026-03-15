"""
Panda Datafreshness Dashboard — improved interactive/business layout

What changed (high level)
- Added an "Is Teams notification sent" column (per-run) and a PagerDuty placeholder column.
- Per-run "Send Teams" and "Send PagerDuty" buttons (placeholders) that toggle state and show badges.
- KPI cards center-aligned, visually stronger styling.
- Failed card highlights:
    - green when there are zero failures,
    - red when >= 1 failure.
- Success KPI shows both count and success rate (centered).
- Improved sparkline/area visuals, tooltip, and compact layout.
- Defensive handling if Success/Failed series are missing.
- Uses st.session_state for per-run notification states, persisted for the session.
"""
import os
from datetime import datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st
import snowflake.connector

st.set_page_config(layout="wide", page_title="Panda Datafreshness Dashboard")

# ---------------------------
# Styling
# ---------------------------
st.markdown(
    """
    <style>
      .kpi-card {
        border-radius:10px;
        padding:18px;
        color: #111;
        box-shadow: 0 3px 10px rgba(0,0,0,0.06);
        text-align:center;
      }
      .kpi-title { font-size:13px; color:#666; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.6px; }
      .kpi-value { font-size:34px; font-weight:800; margin-bottom:6px; }
      .kpi-sub { font-size:12px; color:#666; }
      .blink-dot {
        display:inline-block;
        width:10px;
        height:10px;
        margin-right:8px;
        border-radius:50%;
        background:#2ca02c;
        animation: blink 1.6s infinite;
      }
      @keyframes blink {
        0% { opacity:1; transform: scale(1); }
        50% { opacity:0.25; transform: scale(0.85); }
        100% { opacity:1; transform: scale(1); }
      }
      .teams-badge { font-weight:700; color:#0a66c2; }
      .pagerduty-badge { font-weight:700; color:#ff4d4f; }
      .card-link { text-decoration:none; color:inherit; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------
# Snowflake helpers
# ---------------------------
def _creds_from_env() -> dict:
    return {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
    }


def get_snowflake_connection():
    try:
        creds = dict(st.secrets["snowflake"])
    except Exception:
        creds = _creds_from_env()

    required = ["user", "password", "account"]
    missing = [k for k in required if not creds.get(k)]
    if missing:
        st.error(
            "Snowflake credentials not found. Please set st.secrets['snowflake'] or environment variables: "
            + ", ".join(f"SNOWFLAKE_{k.upper()}" for k in missing)
        )
        st.stop()

    try:
        conn = snowflake.connector.connect(
            user=creds.get("user"),
            password=creds.get("password"),
            account=creds.get("account"),
            warehouse=creds.get("warehouse"),
            database=creds.get("database"),
            schema=creds.get("schema"),
            role=creds.get("role"),
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()


@st.cache_data(ttl=30)
def run_query(sql: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df.columns = df.columns.str.lower()
        return df
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


# ---------------------------
# Top controls and title
# ---------------------------
now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
title_col, spacer, ctrl_col = st.columns([6, 2, 1])
with title_col:
    st.markdown(
        f"### Panda Datafreshness Dashboard  <span class='blink-dot'></span><small style='color:#666'>as of {now_ts}</small>",
        unsafe_allow_html=True,
    )
with ctrl_col:
    if st.button("Refresh"):
        try:
            run_query.clear()
        except Exception:
            pass
        st.rerun()

# date range controls
col_start, col_end, gap = st.columns([1, 1, 6])
with col_start:
    end_date = st.date_input("End date", datetime.utcnow().date())
with col_end:
    start_date = st.date_input("Start date", end_date - timedelta(days=13))
if start_date > end_date:
    st.error("Start date must be <= End date")

# ---------------------------
# Fetch data
# ---------------------------
START_DATE = start_date
END_DATE = end_date

sql = f"""
SELECT
  run_id,
  job_id,
  job_name,
  description,
  owner,
  server_name,
  environment,
  started_at,
  finished_at,
  status,
  runtime_seconds,
  attempts,
  error_code,
  logs
FROM job_run_summary
WHERE started_at::date BETWEEN '{START_DATE.isoformat()}' AND '{END_DATE.isoformat()}'
ORDER BY started_at DESC
LIMIT 5000
"""

try:
    df = run_query(sql)
    data_source = "live data"
except Exception:
    st.warning("Could not load job_run_summary view. Using demo data.")
    data_source = "demo data"
    import random

    rows = []
    jobs = [
        "daily_etl_customers",
        "hourly_metrics_rollup",
        "weekly_purge_old_data",
        "adhoc_manual_report",
        "realtime_stream_processor",
        "dev_test_job",
        "monthly_billing_export",
    ]
    servers = ["sf-prod-01", "sf-staging-01", "sf-dev-01", "sf-analytics-01"]
    days = (END_DATE - START_DATE).days + 1
    for i in range(300):
        started = datetime.utcnow() - timedelta(days=random.randint(0, max(days - 1, 0)), minutes=random.randint(0, 1440))
        finished = started + timedelta(seconds=random.randint(10, 3600))
        status = random.choices(["SUCCESS", "FAILED", "CANCELLED"], weights=[75, 20, 5])[0]
        rows.append(
            {
                "run_id": 1000 + i,
                "job_id": random.randint(1, 8),
                "job_name": random.choice(jobs),
                "description": "demo",
                "owner": random.choice(["alice", "bob", "charlie", "dave"]),
                "server_name": random.choice(servers),
                "environment": random.choice(["prod", "staging", "dev", "analytics"]),
                "started_at": started,
                "finished_at": finished,
                "status": status,
                "runtime_seconds": int((finished - started).total_seconds()),
                "attempts": random.randint(1, 3),
                "error_code": None if status == "SUCCESS" else random.choice(["ERR001", "TIMEOUT", "DB_CONN"]),
                "logs": "Sample log output" if status != "SUCCESS" else None,
            }
        )
    df = pd.DataFrame(rows)
    df.columns = df.columns.str.lower()

# ensure datetimes
if "started_at" in df.columns:
    df["started_at"] = pd.to_datetime(df["started_at"]).dt.tz_localize(None, ambiguous="infer", nonexistent="shift_forward")

# ---------------------------
# Derived metrics
# ---------------------------
total_jobs = int(df["job_name"].nunique()) if "job_name" in df.columns else 0
success_count = int((df["status"] == "SUCCESS").sum()) if "status" in df.columns else 0
failed_count = int((df["status"] == "FAILED").sum()) if "status" in df.columns else 0
attempted = success_count + failed_count
success_rate = round(100 * success_count / attempted, 1) if attempted > 0 else 0

# daily pivot
trend = (
    df.assign(day=df["started_at"].dt.date)
    .groupby(["day", "status"])
    .size()
    .reset_index(name="count")
    .pivot(index="day", columns="status", values="count")
    .fillna(0)
    .reset_index()
)
all_days = pd.DataFrame({"day": [START_DATE + timedelta(days=i) for i in range((END_DATE - START_DATE).days + 1)]})
trend = all_days.merge(trend, on="day", how="left").fillna(0)
if "SUCCESS" not in trend.columns:
    trend["SUCCESS"] = 0
if "FAILED" not in trend.columns:
    trend["FAILED"] = 0


def spark_df_for(series_name: str):
    if series_name in trend.columns:
        return trend[["day", series_name]].rename(columns={series_name: "value"})
    return pd.DataFrame({"day": trend["day"], "value": [0] * len(trend)})


def small_sparkline(source_df, color):
    chart = (
        alt.Chart(source_df)
        .mark_line(interpolate="monotone", strokeWidth=2, color=color)
        .encode(x=alt.X("day:T", axis=None), y=alt.Y("value:Q", axis=None), tooltip=[alt.Tooltip("day:T"), alt.Tooltip("value:Q")])
        .properties(height=60, width=260)
    )
    area = (
        alt.Chart(source_df)
        .mark_area(opacity=0.14, interpolate="monotone", color=color)
        .encode(x="day:T", y=alt.Y("value:Q"))
        .properties(height=60, width=260)
    )
    return area + chart


# ---------------------------
# Notifications session state
# ---------------------------
if "teams_sent" not in st.session_state:
    st.session_state["teams_sent"] = False
if "per_run_sent" not in st.session_state:
    st.session_state["per_run_sent"] = {}  # run_id -> bool
if "per_run_pagerduty" not in st.session_state:
    st.session_state["per_run_pagerduty"] = {}  # run_id -> bool

# ---------------------------
# KPI cards - center aligned + dynamic coloring
# ---------------------------
c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.2, 0.9])

# decide failed card color
if failed_count == 0:
    failed_bg = "#e8fbec"  # greenish
    failed_color = "#1f7a1f"
else:
    failed_bg = "#fff1f0"  # reddish
    failed_color = "#d62728"

with c1:
    st.markdown("<div class='kpi-card' style='background:#f4f6fb;'>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-title'>Total jobs</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='kpi-value'>{total_jobs}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='kpi-sub'>Unique jobs — <small style='color:#666'>{data_source}</small></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with c2:
    st.markdown(f"<div class='kpi-card' style='background:#f2fff6'>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-title'>Successful runs (period)</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='kpi-value' style='color:green'>✔ {success_count}  <span style='font-size:18px; font-weight:700'> {success_rate}%</span></div>", unsafe_allow_html=True)
    st.altair_chart(small_sparkline(spark_df_for("SUCCESS"), "#2ca02c"), use_container_width=False)
    st.markdown("</div>", unsafe_allow_html=True)

with c3:
    st.markdown(f"<div class='kpi-card' style='background:{failed_bg}'>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-title'>Failed runs (period)</div>", unsafe_allow_html=True)
    failed_text = f"✖ {failed_count}"
    if st.session_state["teams_sent"]:
        failed_text += " — 📣 Teams sent"
    st.markdown(f"<div class='kpi-value' style='color:{failed_color}'>{failed_text}</div>", unsafe_allow_html=True)
    st.altair_chart(small_sparkline(spark_df_for("FAILED"), "#d62728"), use_container_width=False)
    st.markdown("</div>", unsafe_allow_html=True)

with c4:
    st.markdown("<div class='kpi-card' style='background:#f7f7ff'>", unsafe_allow_html=True)
    avg_runtime = int(df["runtime_seconds"].dropna().mean()) if "runtime_seconds" in df.columns and not df["runtime_seconds"].dropna().empty else 0
    st.markdown("<div class='kpi-title'>Avg runtime (s)</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='kpi-value'>{avg_runtime}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("---")

# ---------------------------
# Failure-first summary + trend
# ---------------------------
fx1, fx2 = st.columns([2, 3])
with fx1:
    st.subheader("Failure overview (one-shot)")
    st.write(f"- Total failed runs: **{failed_count}**")
    st.write(f"- Success rate: **{success_rate}%**")
    if failed_count > 0:
        top_fails = df[df["status"] == "FAILED"].groupby("job_name").size().reset_index(name="fails").sort_values("fails", ascending=False).head(8)
        st.write("Top failing jobs:")
        st.table(top_fails.rename(columns={"job_name": "Job", "fails": "Fail count"}).set_index("Job"))
        if st.button("Send Teams (global)"):
            st.session_state["teams_sent"] = True
            st.rerun()
        if st.button("Send PagerDuty (global)"):
            # Placeholder only toggles UI; real integration would call PagerDuty API
            for rid in df[df["status"] == "FAILED"]["run_id"].astype(str).tolist():
                st.session_state["per_run_pagerduty"][rid] = True
            st.rerun()
    else:
        st.success("No failures — good job!")

with fx2:
    st.subheader("14-day trend — Success vs Failed")
    trend_melt = trend.melt(id_vars=["day"], value_vars=[c for c in trend.columns if c != "day"], var_name="status", value_name="count")
    line = (
        alt.Chart(trend_melt)
        .mark_line(point=True)
        .encode(
            x="day:T",
            y=alt.Y("count:Q", title="runs"),
            color=alt.Color("status:N", scale=alt.Scale(domain=["SUCCESS", "FAILED"], range=["#2ca02c", "#d62728"])),
            tooltip=["day:T", "status:N", "count:Q"],
        )
        .properties(height=280)
    )
    area = (
        alt.Chart(trend_melt)
        .mark_area(opacity=0.08)
        .encode(x="day:T", y="count:Q", color=alt.Color("status:N", legend=None, scale=alt.Scale(domain=["SUCCESS", "FAILED"], range=["#2ca02c", "#d62728"])))
    )
    st.altair_chart(area + line, use_container_width=True)

st.markdown("---")

# ---------------------------
# Failed runs table with Teams / PagerDuty columns
# ---------------------------
st.subheader("Failed runs — details & actions")
if failed_count == 0:
    st.info("No failed runs in the selected period.")
else:
    failed_df = df[df["status"] == "FAILED"].copy()
    # ensure datetimes
    if "started_at" in failed_df.columns:
        failed_df["started_at"] = pd.to_datetime(failed_df["started_at"])
    if "finished_at" in failed_df.columns:
        failed_df["finished_at"] = pd.to_datetime(failed_df["finished_at"])

    # per-run notification columns (derive from session_state)
    def teams_sent_for(rid):
        return st.session_state["per_run_sent"].get(str(rid), st.session_state["teams_sent"])

    def pagerduty_sent_for(rid):
        return st.session_state["per_run_pagerduty"].get(str(rid), False)

    failed_df["teams_sent"] = failed_df["run_id"].apply(lambda r: teams_sent_for(r))
    failed_df["pagerduty_sent"] = failed_df["run_id"].apply(lambda r: pagerduty_sent_for(r))

    # prepare display table
    display_cols = ["run_id", "job_name", "server_name", "environment", "started_at", "runtime_seconds", "error_code", "teams_sent", "pagerduty_sent"]
    display_cols = [c for c in display_cols if c in failed_df.columns]
    table = failed_df[display_cols].sort_values("started_at", ascending=False).reset_index(drop=True)
    # present nicely
    table["started_at"] = table["started_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
    st.dataframe(table, use_container_width=True)

    st.markdown("### Actions & logs (expand per run)")
    # show interactive expanders with per-run action buttons
    for _, row in failed_df.sort_values("started_at", ascending=False).iterrows():
        rid = row["run_id"]
        label = f"Run {rid} — {row.get('job_name')} — {pd.to_datetime(row.get('started_at')).strftime('%Y-%m-%d %H:%M:%S')}"
        with st.expander(label):
            colA, colB, colC = st.columns([3, 1, 1])
            with colA:
                st.write(f"Server: {row.get('server_name')}")
                st.write(f"Environment: {row.get('environment')}")
                st.write(f"Runtime (s): {row.get('runtime_seconds')}")
                st.write(f"Error code: {row.get('error_code')}")
            with colB:
                key_team = f"team_{rid}"
                if st.session_state["per_run_sent"].get(str(rid), False):
                    st.success("📣 Teams sent")
                    if st.button(f"Undo Teams {rid}", key=f"undo_team_{rid}"):
                        st.session_state["per_run_sent"].pop(str(rid), None)
                        st.rerun()
                else:
                    if st.button(f"Send Teams {rid}", key=key_team):
                        st.session_state["per_run_sent"][str(rid)] = True
                        st.rerun()
            with colC:
                key_pd = f"pd_{rid}"
                if st.session_state["per_run_pagerduty"].get(str(rid), False):
                    st.markdown("<div class='pagerduty-badge'>PagerDuty triggered</div>", unsafe_allow_html=True)
                    if st.button(f"Undo PD {rid}", key=f"undo_pd_{rid}"):
                        st.session_state["per_run_pagerduty"].pop(str(rid), None)
                        st.rerun()
                else:
                    if st.button(f"Trigger PagerDuty {rid}", key=key_pd):
                        st.session_state["per_run_pagerduty"][str(rid)] = True
                        st.rerun()
            # logs
            logs = row.get("logs") or "No logs available"
            st.code(logs)

    # download CSV for failed runs
    csv_bytes = failed_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download failed runs CSV", data=csv_bytes, file_name="failed_runs.csv", mime="text/csv")

st.markdown("---")
st.caption(f"Data source: {data_source} — period: {START_DATE} to {END_DATE}")