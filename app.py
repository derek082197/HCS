import streamlit as st
import pandas as pd
import sqlite3
import io
import zipfile
import csv
from datetime import date, datetime, timedelta
from fpdf import FPDF
import requests
try:
    from streamlit_extras.st_autorefresh import st_autorefresh
except ImportError:
    def st_autorefresh(*args, **kwargs): pass

# 1) PAGE CONFIG — must be first
st.set_page_config(page_title="HCS Commission CRM", layout="wide")

# USERS CSV LOGIN
df_users = pd.read_csv("users.csv", dtype=str).dropna()
USERS = dict(zip(df_users.username.str.strip(), df_users.password))

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def do_login():
    u, p = st.session_state.user.strip(), st.session_state.pwd
    if u in USERS and p == USERS[u]:
        st.session_state.logged_in = True
        st.success(f"✅ Welcome, {u}!")
    else:
        st.error("❌ Incorrect credentials")

def do_logout():
    st.session_state.logged_in = False
    st.experimental_rerun()

if not st.session_state.logged_in:
    st.sidebar.title("🔒 HCS CRM Login")
    st.sidebar.text_input("Username", key="user")
    st.sidebar.text_input("Password", type="password", key="pwd")
    st.sidebar.button("Log in", on_click=do_login)
    st.stop()
st.sidebar.button("Log out", on_click=do_logout)

# CONSTANTS
PROFIT_PER_SALE = 43.3
CRM_API_URL     = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID      = "310"
CRM_API_KEY     = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"
DB              = "crm_history.db"

# DATABASE HELPERS
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
    for col in ["total_deals","agent_payout","owner_revenue","owner_profit"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

# PDF GENERATORS
def generate_agent_pdf(df_agent, agent_name):
    def fix(s):
        return str(s).encode('latin1', errors='replace').decode('latin1')
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial","B",16)
    pdf.cell(0,10,fix("Health Connect Solutions"), ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,10,fix(f"Commission Statement - {agent_name}"), ln=True)
    pdf.ln(5)
    total_deals = len(df_agent)
    paid_count  = (df_agent["Paid Status"]=="Paid").sum()
    unpaid_count= total_deals - paid_count
    pct_paid = (paid_count / total_deals * 100) if total_deals else 0
    if paid_count >= 200: rate = 25
    elif paid_count >= 150: rate = 22.5
    elif paid_count >= 120: rate = 17.5
    else: rate = 15
    bonus  = 1200 if paid_count >= 70 else 0
    payout = paid_count * rate + bonus
    pdf.set_font("Arial","",12)
    pdf.cell(0,8,fix(f"Total Deals Submitted: {total_deals}"), ln=True)
    pdf.cell(0,8,fix(f"Paid Deals: {paid_count}"), ln=True)
    pdf.cell(0,8,fix(f"Unpaid Deals: {unpaid_count}"), ln=True)
    pdf.cell(0,8,fix(f"Paid Percentage: {pct_paid:.1f}%"), ln=True)
    pdf.cell(0,8,fix(f"Rate: ${rate:.2f}"), ln=True)
    pdf.cell(0,8,fix(f"Bonus: ${bonus}"), ln=True)
    pdf.set_text_color(0,150,0)
    pdf.cell(0,10,fix(f"Payout: ${payout:,.2f}"), ln=True)
    pdf.set_text_color(0,0,0)
    pdf.ln(5)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,8,fix("Paid Clients:"), ln=True)
    pdf.set_font("Arial","",10)
    for _, row in df_agent[df_agent["Paid Status"]=="Paid"].iterrows():
        eff = row.get("Effective Date")
        eff_str = eff.strftime("%Y-%m-%d") if pd.notna(eff) else "N/A"
        pdf.multi_cell(0,6,fix(f"- {row['Client']} | Eff: {eff_str}"))
    pdf.ln(3)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,8,fix("Unpaid Clients & Reasons:"), ln=True)
    pdf.set_font("Arial","",10)
    for _, row in df_agent[df_agent["Paid Status"]!="Paid"].iterrows():
        eff = row.get("Effective Date")
        eff_str = eff.strftime("%Y-%m-%d") if pd.notna(eff) else "N/A"
        reason  = row.get("Reason","")
        pdf.multi_cell(0,6,fix(f"- {row['Client']} | Eff: {eff_str} | {reason}"))
    return pdf.output(dest="S").encode("latin1")

def vendor_pdf(paid, unpaid, vendor, rate):
    def fix(s):
        return str(s).encode('latin1', errors='replace').decode('latin1')
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, fix(f"Vendor Pay Summary – {vendor}"), ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, fix(f"Paid Clients"), ln=True)
    pdf.set_font("Arial", "", 10)
    for _, row in paid.iterrows():
        pdf.cell(0, 8, fix(f"- {row['First Name']} {row['Last Name']} | Payout: ${rate}"), ln=True)
    pdf.ln(3)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, fix("Unpaid Clients & Reasons"), ln=True)
    pdf.set_font("Arial", "", 10)
    for _, row in unpaid.iterrows():
        reason = row['Reason'] if 'Reason' in row and pd.notnull(row['Reason']) else ''
        pdf.multi_cell(0, 8, fix(f"- {row['First Name']} {row['Last Name']} | Reason: {reason or 'No reason provided'}"))
    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, fix(f"Totals: {len(paid)} paid (${len(paid)*rate}), {len(unpaid)} unpaid"), ln=True)
    return pdf.output(dest="S").encode("latin1")

def fetch_all_today(limit=5000):
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    params = {
        "date_from": date.today().strftime("%Y-%m-%d"),
        "limit": limit
    }
    all_results, url, seen = [], CRM_API_URL, set()
    while url and url not in seen:
        seen.add(url)
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        js = r.json().get("response", {})
        chunk = js.get("results", [])
        if not chunk:
            break
        all_results.extend(chunk)
        nxt = js.get("navigate", {}).get("next")
        if not nxt or nxt in seen:
            break
        url = nxt
        params = {}
    return pd.DataFrame(all_results)

# INITIALIZATION
init_db()
history_df    = load_history()
summary       = []
uploaded_file = None
threshold     = 10

tabs = st.tabs([
    "🏆 Overview", "📋 Leaderboard", "📈 History",
    "📊 Live Counts", "⚙️ Settings", "📂 Clients", "💼 Vendor Pay"
])

# SETTINGS TAB
with tabs[4]:
    st.header("⚙️ Settings & Upload")
    uploaded_file = st.file_uploader("📥 Upload Excel Statement", type="xlsx")
    threshold     = st.slider("Coaching threshold (Paid Deals)", 0, 100, threshold)
    if uploaded_file:
        st.success("✅ Statement uploaded, processing…")
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
                rate    = 25 if paid_ct>=200 else 22.5 if paid_ct>=150 else 17.5 if paid_ct>=120 else 15
                bonus   = 1200 if paid_ct>=70 else 0
                payout  = paid_ct * rate + bonus
                owner_rev  = paid_ct * 150
                owner_prof = paid_ct * 43
                totals["deals"]     += paid_ct
                totals["agent"]     += payout
                totals["owner_rev"] += owner_rev
                totals["owner_prof"]+= owner_prof
                summary.append({
                    "Agent": agent,
                    "Paid Deals": paid_ct,
                    "Agent Payout": payout,
                    "Owner Profit": owner_prof
                })
                pdf_bytes = generate_agent_pdf(sub, agent)
                zf.writestr(f"{agent.replace(' ','_')}_Paystub.pdf", pdf_bytes)
            # Write admin summary CSV
            csv_buf = io.StringIO()
            w = csv.writer(csv_buf)
            w.writerow(["Agent","Paid Deals","Agent Payout","Owner Profit"])
            for r in summary:
                w.writerow([r["Agent"], r["Paid Deals"], r["Agent Payout"], r["Owner Profit"]])
            zf.writestr("HCS_Admin_Summary.csv", csv_buf.getvalue())
        default_dt = (df["Effective Date"].max().date()
                      if "Effective Date" in df else date.today())
        insert_report(default_dt.strftime("%Y-%m-%d"), totals)
        st.download_button(
            "📦 Download ZIP of Pay Stubs",
            buf.getvalue(),
            file_name=f"paystubs_{datetime.now():%Y%m%d}.zip",
            mime="application/zip"
        )

# OVERVIEW TAB
with tabs[0]:
    st.title("HCS Commission Dashboard")
    if uploaded_file:
        deals = int(totals["deals"])
        c1, c2, c3, c4 = st.columns(4, gap="large")
        c1.metric("Total Paid Deals", f"{deals:,}")
        c2.metric("Agent Payout",    f"${totals['agent']:,.2f}")
        c3.metric("Owner Revenue",   f"${totals['owner_rev']:,.2f}")
        c4.metric("Owner Profit",    f"${totals['owner_prof']:,.2f}")
    else:
        if history_df.empty:
            st.info("Upload a statement to see metrics.")
        else:
            latest = history_df.iloc[-1]
            deals = int(latest.total_deals)
            c1, c2, c3, c4 = st.columns(4, gap="large")
            c1.metric("Total Paid Deals", f"{deals:,}")
            c2.metric("Agent Payout",    f"${latest.agent_payout:,.2f}")
            c3.metric("Owner Revenue",   f"${latest.owner_revenue:,.2f}")
            c4.metric("Owner Profit",    f"${latest.owner_profit:,.2f}")
    st.markdown("---")
    rev = (totals["owner_rev"] if uploaded_file else
           (history_df.iloc[-1].owner_revenue if not history_df.empty else 0))
    s1, s2, s3 = st.columns(3, gap="large")
    s1.metric("Eddy (0.5%)", f"${rev*0.005:,.2f}")
    s2.metric("Matt (2%)",   f"${rev*0.02:,.2f}")
    s3.metric("Jarad (1%)",  f"${rev*0.01:,.2f}")

# LEADERBOARD TAB
with tabs[1]:
    st.header("Agent Leaderboard & Drill-Down")
    if summary:
        df_led = pd.DataFrame(summary).sort_values("Paid Deals", ascending=False)
        st.dataframe(df_led.style.format({
            "Agent Payout":"${:,.2f}",
            "Owner Profit":"${:,.2f}"
        }), use_container_width=True)
        low     = st.slider("Highlight agents below deals:", 0, int(df_led["Paid Deals"].max()), threshold)
        flagged = df_led[df_led["Paid Deals"]<low]
        st.write(f"Agents below {low}: {len(flagged)}")
        if not flagged.empty:
            st.dataframe(flagged, use_container_width=True)
    else:
        st.info("No data—upload in Settings first.")

# HISTORY TAB
with tabs[2]:
    st.header("Historical Reports")
    if history_df.empty:
        st.info("No history yet.")
    else:
        dates = history_df["upload_date"].dt.strftime("%Y-%m-%d").tolist()
        sel   = st.selectbox("View report:", dates)
        rec   = history_df.loc[history_df["upload_date"].dt.strftime("%Y-%m-%d")==sel].iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Deals",        f"{int(rec.total_deals):,}")
        c2.metric("Agent Payout", f"${rec.agent_payout:,.2f}")
        c3.metric("Owner Revenue",f"${rec.owner_revenue:,.2f}")
        c4.metric("Owner Profit", f"${rec.owner_profit:,.2f}")
        st.line_chart(history_df.set_index("upload_date")[["total_deals","agent_payout","owner_revenue","owner_profit"]])

# LIVE COUNTS TAB
with tabs[3]:
    st_autorefresh(interval=10 * 1000, key="live_counts_refresh")
    st.header("Live Daily/Weekly/Monthly/Yearly Counts")
    with st.spinner("Fetching today's leads..."):
        df_api = fetch_all_today(limit=5000)
    if df_api.empty:
        st.error("No leads returned from API.")
    else:
        import pytz
        df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
        if df_api["date_sold"].dt.tz is None or str(df_api["date_sold"].dt.tz) == "None":
            df_api["date_sold"] = df_api["date_sold"].dt.tz_localize('UTC')
        df_api["date_sold"] = df_api["date_sold"].dt.tz_convert('US/Eastern')
        today = pd.Timestamp.now(tz='US/Eastern').date()
        start_of_week = today - timedelta(days=today.weekday())
        this_month = today.replace(day=1)
        this_year = today.replace(month=1, day=1)
        daily_mask   = df_api["date_sold"].dt.date == today
        weekly_mask  = df_api["date_sold"].dt.date >= start_of_week
        monthly_mask = df_api["date_sold"].dt.date >= this_month
        yearly_mask  = df_api["date_sold"].dt.date >= this_year
        d_tot = len(df_api[daily_mask])
        w_tot = len(df_api[weekly_mask])
        m_tot = len(df_api[monthly_mask])
        y_tot = len(df_api[yearly_mask])
        c1, c2, c3, c4 = st.columns(4, gap="large")
        c1.metric("Today's Deals", f"{d_tot:,}")
        c1.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${d_tot * 43:,.2f}</b></span>", unsafe_allow_html=True)
        c2.metric("This Week's Deals", f"{w_tot:,}")
        c2.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${w_tot * 43:,.2f}</b></span>", unsafe_allow_html=True)
        c3.metric("This Month's Deals", f"{m_tot:,}")
        c3.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${m_tot * 43:,.2f}</b></span>", unsafe_allow_html=True)
        c4.metric("This Year's Deals", f"{y_tot:,}")
        c4.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${y_tot * 43:,.2f}</b></span>", unsafe_allow_html=True)
        st.markdown("---")
        def by_agent(mask):
            col = "lead_vendor_name" if "lead_vendor_name" in df_api.columns else df_api.columns[0]
            return (
                df_api[mask]
                .groupby(col)
                .size()
                .rename("Sales")
                .sort_values(ascending=False)
            )
        b1, b2, b3, b4 = st.columns(4, gap="large")
        b1.subheader("Daily Sales by Agent");   b1.bar_chart(by_agent(daily_mask))
        b2.subheader("Weekly Sales by Agent");  b2.bar_chart(by_agent(weekly_mask))
        b3.subheader("Monthly Sales by Agent"); b3.bar_chart(by_agent(monthly_mask))
        b4.subheader("Yearly Sales by Agent");  b4.bar_chart(by_agent(yearly_mask))
        st.markdown("---")
        st.subheader("Today's Deals Table (Eastern Time)")
        cols_to_show = [
            "policy_id", "lead_first_name", "lead_last_name", "date_sold", "carrier", "product"
        ]
        if "lead_vendor_name" in df_api.columns:
            cols_to_show.append("lead_vendor_name")
        st.dataframe(
            df_api[daily_mask][cols_to_show].sort_values("date_sold"),
            use_container_width=True
        )

# CLIENTS TAB (ALL TODAY) with AUTO-REFRESH
with tabs[5]:
    st_autorefresh(interval=10 * 1000, key="clients_tab_refresh")
    st.header("📂 Live Client Leads (Sold Today)")
    df_api = fetch_all_today(limit=5000)
    if df_api.empty:
        st.info("No API leads returned.")
        api_display = pd.DataFrame()
    else:
        df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
        api_today = df_api[df_api["date_sold"].dt.date == date.today()]
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
        if "lead_id" in api_today.columns:
            api_display["Lead ID"] = api_today["lead_id"].astype(str)
    if "manual_leads" not in st.session_state:
        st.session_state.manual_leads = pd.DataFrame()
    combined = (
        api_display
        if st.session_state.manual_leads.empty
        else pd.concat([api_display, st.session_state.manual_leads], ignore_index=True, sort=False)
    )
    if combined.empty:
        st.warning("No leads to display for today.")
    else:
        st.subheader(f"Showing {len(combined)} total leads")
        st.dataframe(combined, use_container_width=True)

# VENDOR PAY TAB
with tabs[6]:
    st.header("💼 Vendor Pay Summary")

    # These are all your vendors (code name: pretty name)
    VENDOR_CODES = {
        "general": "GENERAL",
        "inbound": "INBOUND",
        "sms": "SMS",
        "advancegro": "Advance gro",
        "axad": "AXAD",
        "googlecalls": "GOOGLE CALLS",
        "buffercall": "Aetna",
        "ancletadvising": "Anclet advising",
        "blmcalls": "BLM CALLS",
        "loopcalls": "LOOP CALLS",
        "nobufferaca": "NO BUFFER ACA",
        "raycalls": "RAY CALLS",
        "nomiaca": "Nomi ACA",
        "hcsmedia": "HCS MEDIA",
        "francalls": "Fran Calls",
        "acaking": "ACA KING",
        "ptacacalls": "PT ACA CALLS",
        "hcscaa": "HCS CAA",
        "slavaaca": "Slava ACA",
        "slavaaca2": "Slava ACA 2",
        "francallssupp": "Fran Calls SUPP",
        "derekinhousefb": "DEREK INHOUSE FB",
        "allicalladdoncall": "ALI CALL ADDON CALL",
        "joshaca": "JOSH ACA",
        "hcs1p": "HCS1p"
    }

    # Assign rates to each vendor code (add more as needed)
    VENDOR_RATES = {
        "francalls": 65,
        "hcsmedia": 55,
        "buffercall": 80,      # Aetna
        "acaking": 75,
        # ...add more here if you pay other vendors!
    }

    def normalize_key(x):
        return str(x).strip().lower().replace(' ', '').replace('/', '').replace('_', '')

    tld_file = st.file_uploader("Upload TLD CSV (new/PHI export)", type=["csv"], key="vendor_tld")
    fmo_file = st.file_uploader("Upload FMO Statement (xlsx)", type=["xlsx"], key="vendor_fmo")

    if tld_file and fmo_file:
        st.success("Both files uploaded. Generating vendor ZIP...")

        # Load and normalize vendor names from TLD
        tld = pd.read_csv(tld_file, dtype=str)
        tld['VendorRaw'] = tld.iloc[:, 8].astype(str)
        tld['First Name'] = tld.iloc[:, 3].astype(str)
        tld['Last Name'] = tld.iloc[:, 4].astype(str)

        # Normalize vendor codes in TLD
        tld['vendor_key'] = tld['VendorRaw'].apply(normalize_key)

        fmo = pd.read_excel(fmo_file, dtype=str)
        fmo['First Name'] = fmo.iloc[:, 7].astype(str)
        fmo['Last Name'] = fmo.iloc[:, 8].astype(str)
        fmo['Advance'] = pd.to_numeric(fmo['Advance'], errors='coerce').fillna(0)
        fmo['Reason'] = fmo.get('Advance Excluded Reason', "")
        tld['full_name'] = (tld['First Name'] + tld['Last Name']).apply(normalize_key)
        fmo['full_name'] = (fmo['First Name'] + fmo['Last Name']).apply(normalize_key)

        merged = pd.merge(
            tld,
            fmo[['full_name', 'Advance', 'Reason']],
            on='full_name', how='left'
        )

        # --- Display Vendor Summary Table ---
        vendor_summaries = []
        for vkey, pretty in VENDOR_CODES.items():
            if vkey not in VENDOR_RATES:
                continue
            rate = VENDOR_RATES[vkey]
            sub = merged[merged['vendor_key'] == vkey]
            paid_ct = (sub['Advance'] > 0).sum()
            unpaid_ct = (sub['Advance'] == 0).sum()
            pct_paid = (paid_ct / (paid_ct + unpaid_ct) * 100) if (paid_ct + unpaid_ct) > 0 else 0
            paid_amt = paid_ct * rate
            vendor_summaries.append({
                "Vendor": pretty,
                "Paid Deals": paid_ct,
                "Unpaid Deals": unpaid_ct,
                "Paid %": f"{pct_paid:.1f}%",
                "Total Paid Amount": f"${paid_amt:,.2f}"
            })
        if vendor_summaries:
    df_sum = pd.DataFrame(vendor_summaries)
    st.subheader("Vendor Pay Summary Table")
    st.dataframe(df_sum, use_container_width=True)

    # GRAND TOTAL below the table
    total_paid = sum(
        float(str(row["Total Paid Amount"]).replace("$", "").replace(",", ""))
        for row in vendor_summaries
    )
    st.markdown(
        f"<div style='font-size:1.2em; margin-top:12px; color:#175017;'><b>Total Paid to All Vendors:</b> ${total_paid:,.2f}</div>",
        unsafe_allow_html=True,
    )


        # --- PDF GENERATOR ---
        def vendor_pdf(paid, unpaid, pretty, rate):
            def fix(s): return str(s).encode('latin1', errors='replace').decode('latin1')
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", "B", 14)
            pdf.cell(0, 10, fix(f"Vendor Pay Summary – {pretty}"), ln=True, align="C")
            pdf.ln(3)
            pdf.set_font("Arial", "B", 12)
            # --- Summary stats at top ---
            paid_ct = len(paid)
            unpaid_ct = len(unpaid)
            pct_paid = (paid_ct / (paid_ct + unpaid_ct) * 100) if (paid_ct + unpaid_ct) > 0 else 0
            total_paid_amt = paid_ct * rate
            pdf.cell(0, 8, fix(f"Summary:"), ln=True)
            pdf.set_font("Arial", "", 11)
            pdf.cell(0, 8, fix(f"Paid Deals: {paid_ct}"), ln=True)
            pdf.cell(0, 8, fix(f"Unpaid Deals: {unpaid_ct}"), ln=True)
            pdf.cell(0, 8, fix(f"Paid Percentage: {pct_paid:.1f}%"), ln=True)
            pdf.cell(0, 8, fix(f"Total Paid Amount: ${total_paid_amt:,.2f}"), ln=True)
            pdf.ln(6)

            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, fix(f"Paid Clients"), ln=True)
            pdf.set_font("Arial", "", 10)
            for _, row in paid.iterrows():
                pdf.cell(0, 8, fix(f"- {row['First Name']} {row['Last Name']} | Payout: ${rate}"), ln=True)
            pdf.ln(3)
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, fix("Unpaid Clients & Reasons"), ln=True)
            pdf.set_font("Arial", "", 10)
            for _, row in unpaid.iterrows():
                reason = row['Reason'] if 'Reason' in row and pd.notnull(row['Reason']) else ''
                pdf.multi_cell(0, 8, fix(f"- {row['First Name']} {row['Last Name']} | Reason: {reason or 'No reason provided'}"))
            pdf.ln(5)
            return pdf.output(dest="S").encode("latin1")

        # --- Zip all PDFs ---
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zipf:
            for vkey, pretty in VENDOR_CODES.items():
                if vkey not in VENDOR_RATES:
                    continue
                rate = VENDOR_RATES[vkey]
                sub = merged[merged['vendor_key'] == vkey]
                paid = sub[sub['Advance'] > 0][['First Name', 'Last Name']]
                unpaid = sub[sub['Advance'] == 0][['First Name', 'Last Name', 'Reason']]
                pdf_bytes = vendor_pdf(paid, unpaid, pretty, rate)
                zipf.writestr(f"{pretty.replace(' ', '_')}_Vendor_Pay.pdf", pdf_bytes)

        st.download_button(
            "Download ZIP of Vendor Pay Reports",
            buf.getvalue(),
            file_name="Vendor_Pay_Summaries.zip",
            mime="application/zip"
        )
        st.info("Each PDF lists only converted deals (matched to FMO). Unpaid reasons included. Vendor rate auto-applied.")

    else:
        st.warning("Please upload both files to generate vendor pay summaries.")




















    






