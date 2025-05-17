import streamlit as st
import pandas as pd
import sqlite3
import io
import zipfile
import csv
import re
from datetime import date, datetime
from fpdf import FPDF
import requests  # for CRM API

# ──────────────────────────────────────────────────────────────────────────────
# 1) PAGE CONFIG — MUST be the VERY FIRST Streamlit call
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="HCS Commission CRM", layout="wide")

# ──────────────────────────────────────────────────────────────────────────────
# 2) CREDENTIAL CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────
APP_USER     = "derek082197"
APP_PASSWORD = "Xd5gihbw!"    # ← must be a quoted string

# ──────────────────────────────────────────────────────────────────────────────
# 3) LOGIN PROMPT + GATE
# ──────────────────────────────────────────────────────────────────────────────
user = st.text_input("Username")
pwd  = st.text_input("Password", type="password")

if not (user == APP_USER and pwd == APP_PASSWORD):
    st.warning("❌ Incorrect credentials")
    st.stop()   # everything below here is locked down until correct login

# ──────────────────────────────────────────────────────────────────────────────
# 4) REST OF YOUR APP (runs only after successful login)

# ---------------------------------------
# LIVE SHEET URL — your published Google sheet CSV
# ---------------------------------------
LIVE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vS1kek7ytwtLfJa6peHTp8WknP4l6oeIH6t0luVLJs9hySW0w-"
    "jPvZZSuy9mO4MJmJFB06-b3wtgNBw/pub?gid=1891837351"
    "&single=true&output=csv"
)

# ---------------------------------------
# Profit per deal constant
# ---------------------------------------
PROFIT_PER_SALE = 43.3

# ---------------------------------------
# TLD CRM API settings
# ---------------------------------------
CRM_API_URL  = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID   = "310"
CRM_API_KEY  = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"

# ---------------------------------------
# DATABASE HELPERS
# ---------------------------------------
DB = "crm_history.db"

def init_db():
    conn = sqlite3.connect(DB)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS reports (
        upload_date TEXT PRIMARY KEY,
        total_deals INTEGER,
        agent_payout REAL,
        owner_revenue REAL,
        owner_profit REAL
      )
    """)
    conn.commit()
    conn.close()

def insert_report(dt, totals):
    conn = sqlite3.connect(DB)
    conn.execute("""
      INSERT OR REPLACE INTO reports
      (upload_date, total_deals, agent_payout, owner_revenue, owner_profit)
      VALUES (?, ?, ?, ?, ?)
    """, (dt, totals["deals"], totals["agent"], totals["owner_rev"], totals["owner_prof"]))
    conn.commit()
    conn.close()

@st.cache_data
def load_history():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT * FROM reports ORDER BY upload_date", conn, parse_dates=["upload_date"])
    conn.close()
    return df

# ---------------------------------------
# PDF GENERATOR
# ---------------------------------------
def generate_agent_pdf(df_agent, agent_name):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "Health Connect Solutions", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, f"Commission Statement - {agent_name}", ln=True)
    pdf.ln(5)

    total_deals = len(df_agent)
    paid_count  = (df_agent["Paid Status"] == "Paid").sum()
    unpaid_count = total_deals - paid_count

    # tiered rate
    if paid_count >= 200:
        rate = 25
    elif paid_count >= 150:
        rate = 22.5
    elif paid_count >= 120:
        rate = 17.5
    else:
        rate = 15

    bonus  = 1200 if paid_count >= 70 else 0
    payout = paid_count * rate + bonus

    pdf.set_font("Arial", "", 12)
    pdf.cell(0, 8, f"Total Deals Submitted: {total_deals}", ln=True)
    pdf.cell(0, 8, f"Paid Deals: {paid_count}", ln=True)
    pdf.cell(0, 8, f"Unpaid Deals: {unpaid_count}", ln=True)
    pdf.cell(0, 8, f"Rate: ${rate:.2f}", ln=True)
    pdf.cell(0, 8, f"Bonus: ${bonus}", ln=True)
    pdf.set_text_color(0, 150, 0)
    pdf.cell(0, 10, f"Payout: ${payout:,.2f}", ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(5)

    # Paid clients list
    paid_df = df_agent[df_agent["Paid Status"] == "Paid"]
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Paid Clients:", ln=True)
    pdf.set_font("Arial", "", 10)
    for _, row in paid_df.iterrows():
        eff = row.get("Effective Date")
        eff_str = eff.strftime("%Y-%m-%d") if pd.notna(eff) else "N/A"
        pdf.multi_cell(0, 6, f"- {row['Client']} | Eff: {eff_str}")

    # Unpaid clients & reasons
    pdf.ln(3)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Unpaid Clients & Reasons:", ln=True)
    pdf.set_font("Arial", "", 10)
    for _, row in df_agent[df_agent["Paid Status"] != "Paid"].iterrows():
        eff = row.get("Effective Date")
        eff_str = eff.strftime("%Y-%m-%d") if pd.notna(eff) else "N/A"
        reason = str(row.get("Reason", ""))
        pdf.multi_cell(0, 6, f"- {row['Client']} | Eff: {eff_str} | {reason}")
        pdf.ln(1)

    return pdf.output(dest="S").encode("latin1")

# ---------------------------------------
# LIVE COUNTS LOADER
# ---------------------------------------
@st.cache_data(ttl=300)
def load_live_counts():
    df = pd.read_csv(LIVE_SHEET_URL)
    return df.loc[:, ~df.columns.str.contains("^Unnamed")]

# ---------------------------------------
# CRM “Clients” loader w/ pagination
# ---------------------------------------
@st.cache_data(ttl=60)
def load_crm_leads():
    headers = {
        "tld-api-id":  CRM_API_ID,
        "tld-api-key": CRM_API_KEY,
    }
    all_results = []
    url = CRM_API_URL
    seen = set()
    while url and url not in seen:
        seen.add(url)
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        js = r.json().get("response", {})
        results = js.get("results", [])
        if not results:
            break
        all_results.extend(results)
        next_url = js.get("navigate", {}).get("next")
        if not next_url or next_url == url:
            break
        url = next_url
    return pd.DataFrame(all_results)

# ---------------------------------------
# INITIALIZATION
# ---------------------------------------
init_db()
history_df    = load_history()
summary       = []
uploaded_file = None
threshold     = 10

# ---------------------------------------
# TABS SETUP
# ---------------------------------------
tabs = st.tabs([
    "🏆 Overview",
    "📋 Leaderboard",
    "📈 History",
    "📊 Live Counts",
    "⚙️ Settings",
    "📂 Clients",
])

# ---------------------------------------
# SETTINGS TAB
# ---------------------------------------
with tabs[4]:
    st.header("⚙️ Settings & Upload")
    uploaded_file = st.file_uploader("📥 Upload Excel Statement", type="xlsx")
    threshold     = st.slider("Coaching threshold (Paid Deals)", 0, 100, threshold)

    if uploaded_file:
        st.success("✅ Statement uploaded, processing...")
        df = pd.read_excel(uploaded_file)
        df.dropna(subset=["Agent","first_name","last_name","Advance"], inplace=True)
        df["Client"]         = df["first_name"].str.strip() + " " + df["last_name"].str.strip()
        df["Paid Status"]    = df["Advance"].fillna(0).astype(float).apply(lambda x: "Paid" if x>0 else "Not Paid")
        df["Reason"]         = df.get("Advance Excluded Reason","").fillna("").astype(str)
        df["Effective Date"] = pd.to_datetime(df.get("Eff Date"), errors="coerce")

        totals = {"deals":0, "agent":0.0, "owner_rev":0.0, "owner_prof":0.0}
        summary.clear()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for agent in df["Agent"].unique():
                sub     = df[df["Agent"]==agent]
                paid_ct = (sub["Paid Status"]=="Paid").sum()
                if paid_ct >= 200:    rate = 25
                elif paid_ct >= 150:  rate = 22.5
                elif paid_ct >= 120:  rate = 17.5
                else:                 rate = 15
                bonus      = 1200 if paid_ct >= 70 else 0
                payout     = paid_ct * rate + bonus
                owner_rev  = paid_ct * 150
                owner_prof = paid_ct * 43

                totals["deals"]      += paid_ct
                totals["agent"]      += payout
                totals["owner_rev"]  += owner_rev
                totals["owner_prof"] += owner_prof

                summary.append({
                    "Agent": agent,
                    "Paid Deals": paid_ct,
                    "Agent Payout": payout,
                    "Owner Profit": owner_prof
                })

                pdf_bytes = generate_agent_pdf(sub, agent)
                zf.writestr(f"{agent.replace(' ','_')}_Paystub.pdf", pdf_bytes)

            # Admin summary CSV
            csv_buf = io.StringIO()
            w = csv.writer(csv_buf)
            w.writerow(["Agent","Paid Deals","Agent Payout","Owner Profit"])
            for r in summary:
                w.writerow([r["Agent"], r["Paid Deals"], r["Agent Payout"], r["Owner Profit"]])
            zf.writestr("HCS_Admin_Summary.csv", csv_buf.getvalue())

        default_dt = df["Effective Date"].max().date() if "Effective Date" in df else date.today()
        insert_report(default_dt.strftime("%Y-%m-%d"), totals)

        st.download_button(
            "📦 Download ZIP of Pay Stubs",
            buf.getvalue(),
            file_name=f"paystubs_{datetime.now():%Y%m%d}.zip",
            mime="application/zip"
        )

# ---------------------------------------
# OVERVIEW TAB
# ---------------------------------------
with tabs[0]:
    st.title("HCS Commission Dashboard")
    if uploaded_file:
        c1, c2, c3, c4 = st.columns(4, gap="large")
        c1.metric("Total Paid Deals", f"{totals['deals']:,}")
        c2.metric("Agent Payout",      f"${totals['agent']:,.2f}")
        c3.metric("Owner Revenue",     f"${totals['owner_rev']:,.2f}")
        c4.metric("Owner Profit",      f"${totals['owner_prof']:,.2f}")
    else:
        if history_df.empty:
            st.info("Upload a statement to see metrics.")
        else:
            latest = history_df.iloc[-1]
            c1, c2, c3, c4 = st.columns(4, gap="large")
            c1.metric("Total Paid Deals", f"{latest.total_deals:,}")
            c2.metric("Agent Payout",      f"${latest.agent_payout:,.2f}")
            c3.metric("Owner Revenue",     f"${latest.owner_revenue:,.2f}")
            c4.metric("Owner Profit",      f"${latest.owner_profit:,.2f}")
    st.markdown("---")
    rev = (totals["owner_rev"] if uploaded_file else
           (history_df.iloc[-1].owner_revenue if not history_df.empty else 0))
    s1, s2, s3 = st.columns(3, gap="large")
    s1.metric("Eddy (0.5%)", f"${rev * 0.005:,.2f}")
    s2.metric("Matt (2%)",   f"${rev * 0.02:,.2f}")
    s3.metric("Jarad (1%)",  f"${rev * 0.01:,.2f}")

# ---------------------------------------
# LEADERBOARD TAB
# ---------------------------------------
with tabs[1]:
    st.header("Agent Leaderboard & Drill-Down")
    if summary:
        df_led = pd.DataFrame(summary).sort_values("Paid Deals", ascending=False)
        st.dataframe(df_led.style.format({
            "Agent Payout":"${:,.2f}",
            "Owner Profit":"${:,.2f}"
        }), use_container_width=True)
        low = st.slider("Highlight agents below deals:", 0, int(df_led["Paid Deals"].max()), threshold)
        flagged = df_led[df_led["Paid Deals"]<low]
        st.write(f"Agents below {low}: {len(flagged)}")
        if not flagged.empty:
            st.dataframe(flagged, use_container_width=True)
    else:
        st.info("No data—upload in Settings first.")

# ---------------------------------------
# HISTORY TAB
# ---------------------------------------
with tabs[2]:
    st.header("Historical Reports")
    if history_df.empty:
        st.info("No history yet.")
    else:
        dates = history_df["upload_date"].dt.strftime("%Y-%m-%d").tolist()
        to_del = st.multiselect("Delete reports:", dates)
        if st.button("Delete Selected"):
            conn = sqlite3.connect(DB)
            for d in to_del:
                conn.execute("DELETE FROM reports WHERE upload_date=?", (d,))
            conn.commit(); conn.close()
            st.success("Deleted—refresh to update.")
        sel = st.selectbox("View report:", dates)
        rec = history_df[history_df["upload_date"].dt.strftime("%Y-%m-%d")==sel].iloc[0]
        cols = st.columns(4)
        cols[0].metric("Deals",         f"{rec.total_deals:,}")
        cols[1].metric("Agent Payout",  f"${rec.agent_payout:,.2f}")
        cols[2].metric("Owner Revenue", f"${rec.owner_revenue:,.2f}")
        cols[3].metric("Owner Profit",  f"${rec.owner_profit:,.2f}")
        st.line_chart(history_df.set_index("upload_date")[
            ["total_deals","agent_payout","owner_revenue","owner_profit"]
        ])

# ---------------------------------------
# LIVE COUNTS TAB
# ---------------------------------------
with tabs[3]:
    st.header("Live Daily/Weekly/Monthly Counts")
    live_df = load_live_counts()
    tot_rows = live_df[live_df["Agent"].str.upper()=="TOTAL"]
    if len(tot_rows) < 3:
        st.error("Need at least 3 TOTAL rows in your live CSV.")
    else:
        sales = (
            pd.to_numeric(
                tot_rows["Sales"].astype(str).str.replace(r"[^0-9]", "", regex=True),
                errors="coerce"
            ).fillna(0).astype(int)
        )
        d_tot, w_tot, m_tot = sales.iloc[0], sales.iloc[1], sales.iloc[2]
        c1, c2, c3 = st.columns(3, gap="large")
        c1.metric("Today's Deals",      f"{d_tot:,}")
        c2.metric("This Week's Deals",  f"{w_tot:,}")
        c3.metric("This Month's Deals", f"{m_tot:,}")
        c1.metric("Today's Profit",      f"${d_tot*PROFIT_PER_SALE:,.2f}")
        c2.metric("This Week's Profit",  f"${w_tot*PROFIT_PER_SALE:,.2f}")
        c3.metric("This Month's Profit", f"${m_tot*PROFIT_PER_SALE:,.2f}")
        st.markdown("---")
        agents    = live_df["Agent"]
        sales_all = pd.to_numeric(
            live_df["Sales"].astype(str).str.replace(r"[^0-9]", "", regex=True),
            errors="coerce"
        ).fillna(0).astype(int)
        daily_block   = pd.DataFrame({"Agent":agents.iloc[0:58],   "Sales":sales_all.iloc[0:58]}).set_index("Agent").sort_values("Sales", ascending=False)
        weekly_block  = pd.DataFrame({"Agent":agents.iloc[60:118], "Sales":sales_all.iloc[60:118]}).set_index("Agent").sort_values("Sales", ascending=False)
        monthly_block = pd.DataFrame({"Agent":agents.iloc[120:178],"Sales":sales_all.iloc[120:178]}).set_index("Agent").sort_values("Sales", ascending=False)
        b1, b2, b3 = st.columns(3, gap="large")
        b1.subheader("Daily Sales by Agent");   b1.bar_chart(daily_block)
        b2.subheader("Weekly Sales by Agent");  b2.bar_chart(weekly_block)
        b3.subheader("Monthly Sales by Agent"); b3.bar_chart(monthly_block)

# ---------------------------------------
# CLIENTS TAB
# ---------------------------------------
with tabs[5]:
    st.header("📂 Live Client Leads (Sold Today)")

    # Pull today’s leads from API
    if st.button("🔄 Refresh API Leads"):
        load_crm_leads.clear()
    df_api = load_crm_leads()

    # Clean / filter API leads
    if df_api.empty:
        st.info("No API leads returned.")
        api_display = pd.DataFrame()
    else:
        df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
        today = date.today()
        api_today = df_api[df_api["date_sold"].dt.date == today]

        # Only keep the columns that exist
        api_cols = [
            "policy_id","lead_first_name","lead_last_name","lead_state",
            "date_sold","carrier","product","duration","premium",
            "policy_number","lead_vendor_name"
        ]
        api_cols = [c for c in api_cols if c in api_today.columns]

        api_display = api_today[api_cols].rename(columns={
            "policy_id":       "Policy ID",
            "lead_first_name": "First Name",
            "lead_last_name":  "Last Name",
            "lead_state":      "State",
            "date_sold":       "Date Sold",
            "lead_vendor_name":"Vendor",
        })

        # tack on Lead ID if it exists
        if "lead_id" in api_today.columns:
            api_display["Lead ID"] = api_today["lead_id"].astype(str)

    # Initialize manual-upload stash
    if "manual_leads" not in st.session_state:
        st.session_state.manual_leads = pd.DataFrame()

    # Upload & clean historical leads
    st.subheader("📥 Upload Historical Leads")
    uploaded = st.file_uploader("Upload CSV or Excel with a 'lead_id' column", type=["csv", "xlsx"])
    if uploaded:
        # read file
        if uploaded.name.lower().endswith(".csv"):
            df_imp = pd.read_csv(uploaded, dtype=str)
        else:
            df_imp = pd.read_excel(uploaded, dtype=str)

        if "lead_id" not in df_imp.columns:
            st.error("⚠️ Your file needs a `lead_id` column")
        else:
            df_imp["lead_id"] = df_imp["lead_id"].astype(str)
            st.markdown("**Preview imported:**")
            st.dataframe(df_imp, use_container_width=True)

            # delete mistakes
            to_remove = st.multiselect("Select lead_id(s) to drop", df_imp["lead_id"].tolist())
            if st.button("🗑️ Drop selected rows"):
                df_imp = df_imp[~df_imp["lead_id"].isin(to_remove)]
                st.success(f"Dropped {len(to_remove)} rows")
                st.dataframe(df_imp, use_container_width=True)

            # import into CRM (session-state)
            if st.button("✅ Import cleaned leads into CRM"):
                # rename to match API display if you want to show it as "Lead ID"
                st.session_state.manual_leads = df_imp.rename(columns={"lead_id":"Lead ID"})
                st.success(f"Imported {len(df_imp)} leads")

        st.markdown("---")

    # Combine API + manual uploads
    if st.session_state.manual_leads.empty:
        combined = api_display
    else:
        combined = pd.concat(
            [api_display, st.session_state.manual_leads],
            ignore_index=True,
            sort=False
        )

    # Final render
    if combined.empty:
        st.warning("No leads to display for today.")
    else:
        st.subheader(f"Showing {len(combined)} total leads")
        st.dataframe(combined, use_container_width=True)




    






