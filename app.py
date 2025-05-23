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

st.set_page_config(page_title="HCS Commission CRM", layout="wide")

commission_cycles = pd.DataFrame([
    # ("Cycle Start", "Cycle End", "Pay Date")
    ("12/14/24", "12/27/24", "1/3/25"),   ("12/28/24", "1/10/25", "1/17/25"),
    ("1/11/25", "1/24/25", "1/31/25"),    ("1/25/25", "2/7/25", "2/14/25"),
    ("2/8/25", "2/21/25", "2/28/25"),     ("2/22/25", "3/7/25", "3/14/25"),
    ("3/8/25", "3/21/25", "3/28/25"),     ("3/22/25", "4/4/25", "4/11/25"),
    ("4/5/25", "4/18/25", "4/25/25"),     ("4/19/25", "5/2/25", "5/9/25"),
    ("5/3/25", "5/16/25", "5/23/25"),     ("5/17/25", "5/30/25", "6/6/25"),
    ("5/31/25", "6/13/25", "6/20/25"),    ("6/14/25", "6/27/25", "7/3/25"),
    ("6/28/25", "7/11/25", "7/18/25"),    ("7/12/25", "7/25/25", "8/1/25"),
    ("7/26/25", "8/8/25", "8/15/25"),     ("8/9/25", "8/22/25", "8/29/25"),
    ("8/23/25", "9/5/25", "9/12/25"),     ("9/6/25", "9/19/25", "9/26/25"),
    ("9/20/25", "10/3/25", "10/10/25"),   ("10/4/25", "10/17/25", "10/24/25"),
    ("10/18/25", "10/31/25", "11/7/25"),  ("11/1/25", "11/14/25", "11/21/25"),
    ("11/15/25", "11/28/25", "12/5/25"),  ("11/29/25", "12/12/25", "12/19/25"),
    ("12/13/25", "12/26/25", "1/2/26"),   ("12/27/25", "1/9/26", "1/16/26"),
], columns=["start", "end", "pay"])
commission_cycles["start"] = pd.to_datetime(commission_cycles["start"])
commission_cycles["end"] = pd.to_datetime(commission_cycles["end"])
commission_cycles["pay"] = pd.to_datetime(commission_cycles["pay"])

PROFIT_PER_SALE = 43.3
CRM_API_URL     = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID      = "310"
CRM_API_KEY     = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"
DB              = "crm_history.db"

df_users = pd.read_csv("users.csv", dtype=str).dropna()
USERS = dict(zip(df_users.username.str.strip(), df_users.password))
ADMIN_NAMES = dict(zip(df_users.username, [f"{r['first_name']} {r['last_name']}" for _, r in df_users.iterrows()]))
ADMIN_ROLES = dict(zip(df_users.username, df_users.role))

@st.cache_data(ttl=600)
def fetch_agents():
    url = "https://hcs.tldcrm.com/api/egress/users"
    headers = {
        "tld-api-id": CRM_API_ID,
        "tld-api-key": CRM_API_KEY,
    }
    params = {"limit": 1000}
    r = requests.get(url, headers=headers, params=params, timeout=10)
    js = r.json().get('response', {})
    users = js.get('results', [])
    return pd.DataFrame(users)

df_agents = fetch_agents()
AGENT_USERNAMES = df_agents['username'].tolist()
AGENT_CREDENTIALS = {u: 'password' for u in AGENT_USERNAMES}
AGENT_NAMES = dict(zip(df_agents['username'], [f"{row['first_name']} {row['last_name']}" for _, row in df_agents.iterrows()]))
AGENT_ROLES = dict(zip(df_agents['username'], df_agents['role_descriptions']))
AGENT_USERIDS = dict(zip(df_agents['username'], df_agents['user_id']))

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user_role = ""
    st.session_state.user_email = ""
    st.session_state.user_name = ""

def do_login():
    u = st.session_state.user.strip()
    p = st.session_state.pwd
    if u in AGENT_CREDENTIALS and p == AGENT_CREDENTIALS[u]:
        st.session_state.logged_in = True
        st.session_state.user_email = u
        st.session_state.user_name = AGENT_NAMES[u]
        st.session_state.user_role = AGENT_ROLES[u] if AGENT_ROLES.get(u) else "Agent"
        st.success(f"✅ Welcome, {AGENT_NAMES[u]}!")
    elif u in USERS and p == USERS[u]:
        st.session_state.logged_in = True
        st.session_state.user_email = u
        st.session_state.user_name = ADMIN_NAMES.get(u, u)
        st.session_state.user_role = ADMIN_ROLES.get(u, "Admin")
        st.success(f"✅ Welcome, {st.session_state.user_name}! (Admin)")
    else:
        st.error("❌ Incorrect credentials")

def do_logout():
    st.session_state.logged_in = False
    st.session_state.user_role = ""
    st.session_state.user_email = ""
    st.session_state.user_name = ""
    st.experimental_rerun()

if not st.session_state.logged_in:
    st.sidebar.title("🔒 HCS CRM Login")
    st.sidebar.text_input("Username", key="user")
    st.sidebar.text_input("Password", type="password", key="pwd")
    st.sidebar.button("Log in", on_click=do_login)
    st.stop()
st.sidebar.button("Log out", on_click=do_logout)


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

init_db()
history_df = load_history()
summary = []
uploaded_file = None
threshold = 10


# --- Fetch All Deals (for agent dashboards, live counts, etc)
def fetch_all_today(limit=5000):
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    today_str = date.today().strftime("%Y-%m-%d")
    columns = [
        'policy_id', 'date_created', 'date_converted', 'date_sold', 'date_posted',
        'carrier', 'product', 'duration', 'premium', 'policy_number',
        'lead_first_name', 'lead_last_name', 'lead_state', 'lead_vendor_name',
        'agent_id', 'agent_name'
    ]
    params = {
        "date_from": today_str, 
        "limit": limit,
        "columns": ",".join(columns)
    }
    all_results, url, seen = [], CRM_API_URL, set()
    while url and url not in seen:
        seen.add(url)
        try:
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
        except Exception as e:
            st.error(f"API Error: {str(e)}")
            break
    return pd.DataFrame(all_results)

def fetch_agent_deals(user_id, date_from, date_to):
    columns = [
        'policy_id', 'date_sold', 'carrier', 'product', 'premium',
        'lead_first_name', 'lead_last_name', 'lead_state', 'lead_vendor_name',
        'agent_id', 'agent_name'
    ]
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    params = {
        "agent_id": user_id,
        "date_sold_greater_equal": date_from,
        "date_sold_less_equal": date_to,
        "limit": 1000,
        "columns": ",".join(columns)
    }
    resp = requests.get(CRM_API_URL, headers=headers, params=params, timeout=10)
    js = resp.json().get("response", {})
    deals = js.get("results", [])
    # DEBUG: Print your deals for verification
    print("API CALL PARAMS:", params)
    print("API CALL DEALS:", deals[:2])
    df = pd.DataFrame(deals)
    if "date_sold" in df.columns:
        df["date_sold"] = pd.to_datetime(df["date_sold"], errors="coerce")
    return df



# --- PDF GENERATORS
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

# Initialize the database and load history
init_db()
history_df = load_history()
summary = []
uploaded_file = None
threshold = 10

# === AGENT DASHBOARD ===
# --- AGENT LIVE COUNTS (DAY/WEEK/MONTH/YEAR, MATCHING ADMIN LOGIC) ---

# --- AGENT DASHBOARD LOGIC, CLEAN REPLACEMENT BLOCK ---

# === AGENT DASHBOARD ===
# ========== AGENT DASHBOARD ==========

# Add this block after the previous cycle summary in Agent Dashboard!

# --- Net payout from FMO (if available) ---
if st.session_state.user_role.lower() == "agent":
    st.markdown(
        f"""
        <div style="padding:1.5em 1em 0.2em 1em; background: linear-gradient(90deg,#eef5ff,#f5fff0 80%); border-radius:16px;">
            <h1 style='font-size:2.4em; margin-bottom:0; color:#223969;'>
                👤 Agent Dashboard — <span style="color:#208b26;">{st.session_state.user_name}</span>
            </h1>
        </div>
        """, unsafe_allow_html=True,
    )

    agent = df_agents[df_agents['username'] == st.session_state.user_email]
    if agent.empty:
        st.error("Agent not found."); st.stop()
    user_id = str(agent['user_id'].iloc[0])

    # --- Commission Cycle Dates ---
    cycles = commission_cycles.sort_values("start").reset_index(drop=True)
    today = pd.Timestamp.now(tz='US/Eastern').date()
    current_idx = None
    for idx, row in cycles.iterrows():
        if row["start"].date() <= today <= row["end"].date():
            current_idx = idx
            break
    if current_idx is None:
        st.error("No active commission cycle for today."); st.stop()
    prev_idx = current_idx - 1 if current_idx > 0 else None
    current_row = cycles.loc[current_idx]
    cycle_start = current_row["start"].strftime("%Y-%m-%d")
    cycle_end   = current_row["end"].strftime("%Y-%m-%d")
    pay_date    = current_row["pay"].strftime("%m/%d/%y")
    if prev_idx is not None:
        prev_row = cycles.loc[prev_idx]
        prev_start = prev_row["start"].strftime("%Y-%m-%d")
        prev_end   = prev_row["end"].strftime("%Y-%m-%d")
        prev_pay   = prev_row["pay"].strftime("%m/%d/%y")
    else:
        prev_start = prev_end = prev_pay = ""

    # --- TQL API Helper ---
    def fetch_agent_deals(user_id, dfrom, dto):
        columns = [
            'policy_id', 'date_sold', 'carrier', 'product', 'premium',
            'lead_first_name', 'lead_last_name', 'lead_state', 'lead_vendor_name',
            'agent_id', 'agent_name'
        ]
        headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
        params = {
            "agent_id": user_id,
            "date_sold": dfrom,
            "date_sold_end": dto,
            "limit": 1000,
            "columns": ",".join(columns)
        }
        resp = requests.get(CRM_API_URL, headers=headers, params=params, timeout=10)
        js = resp.json().get("response", {})
        deals = js.get("results", [])
        df = pd.DataFrame(deals)
        if "date_sold" in df.columns:
            df["date_sold"] = pd.to_datetime(df["date_sold"], errors="coerce")
        return df

    # --- LIVE COUNTS ---
    today_str    = today.strftime("%Y-%m-%d")
    week_start   = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
    month_start  = today.replace(day=1).strftime("%Y-%m-%d")
    year_start   = today.replace(month=1, day=1).strftime("%Y-%m-%d")
    deals_today  = fetch_agent_deals(user_id, today_str, today_str)
    deals_week   = fetch_agent_deals(user_id, week_start, today_str)
    deals_month  = fetch_agent_deals(user_id, month_start, today_str)
    deals_year   = fetch_agent_deals(user_id, year_start, today_str)
    deals_cycle  = fetch_agent_deals(user_id, cycle_start, cycle_end)

    daily_count   = len(deals_today)
    weekly_count  = len(deals_week)
    monthly_count = len(deals_month)
    yearly_count  = len(deals_year)
    cycle_count   = len(deals_cycle)

    # --- TIER LOGIC ---
    if cycle_count >= 200:
        rate = 25; tier = "Top Tier ($25/deal)";     tier_color = "#13b13b"
    elif cycle_count >= 150:
        rate = 22.5; tier = "Pro Tier ($22.50/deal)"; tier_color = "#26a7ff"
    elif cycle_count >= 120:
        rate = 17.5; tier = "Rising Tier ($17.50/deal)"; tier_color = "#fd9800"
    else:
        rate = 15;   tier = "Starter ($15/deal)";    tier_color = "#a0a0a0"
    bonus = 1200 if cycle_count >= 70 else 0
    payout = cycle_count * rate + bonus

    # Next tier progress
    tier_targets = [(70, "Bonus $1200"), (120, 17.5), (150, 22.5), (200, 25)]
    next_target = None
    for th, v in tier_targets:
        if cycle_count < th:
            next_target = th
            break
    pct_to_next = (cycle_count / next_target * 100) if next_target else 100

    # Bonus progress (to 70)
    bonus_target = 70
    pct_to_bonus = min((cycle_count / bonus_target * 100), 100)

    # --- Previous Cycle: Gross & Net (FMO) ---
    prev_count = prev_payout = prev_rate = prev_bonus = 0
    net_paid = None
    paid_rows = None
    if prev_start and prev_end:
        deals_prev_cycle = fetch_agent_deals(user_id, prev_start, prev_end)
        prev_count = len(deals_prev_cycle)
        if prev_count >= 200: prev_rate = 25
        elif prev_count >= 150: prev_rate = 22.5
        elif prev_count >= 120: prev_rate = 17.5
        else: prev_rate = 15
        prev_bonus = 1200 if prev_count >= 70 else 0
        prev_payout = prev_count * prev_rate + prev_bonus

        # --- FMO NET PAY
        if 'uploaded_file' in locals() and uploaded_file is not None:
            try:
                fmo_df = pd.read_excel(uploaded_file, dtype=str)
                agent_name = st.session_state.user_name.strip().lower()
                agent_rows = fmo_df[fmo_df["Agent"].str.strip().str.lower() == agent_name]
                advance_col = next((c for c in fmo_df.columns if "advance" in c.lower()), "Advance")
                agent_rows[advance_col] = pd.to_numeric(agent_rows[advance_col], errors="coerce").fillna(0)
                net_paid = agent_rows[advance_col][agent_rows[advance_col] == 150].sum() if not agent_rows.empty else 0.0
                paid_rows = agent_rows[agent_rows[advance_col] == 150]
            except Exception as ex:
                net_paid = None
                paid_rows = None

    # === DISPLAY DASHBOARD ===
    st.subheader("Current Commission Cycle")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Deals (Cycle)", cycle_count)
    c2.metric("Projected Payout", f"${payout:,.2f}")
    c3.metric("Cycle", f"{cycle_start} to {cycle_end}")
    c4.metric("Pay Date", f"{pay_date}")

    # --- Tier Progress Bar
    st.markdown(f"""
        <div style="background:{tier_color}22; padding:8px 16px; border-radius:10px; margin:8px 0 0 0;">
            <b style="color:{tier_color}; font-size:1.1em;">{tier}</b>
            <span style='color:#222; font-size:1em; margin-left:16px;'>
                {f'{cycle_count}/{next_target} deals to next tier' if next_target else "MAX tier achieved"}
            </span>
            <div style='background:#e5e5e5;border-radius:8px;height:12px;margin-top:4px;'>
                <div style='background:{tier_color};width:{pct_to_next:.1f}%;height:12px;border-radius:8px;'></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --- Bonus Progress Bar
    st.markdown(f"""
        <div style="background:#eaf6ff; padding:8px 16px; border-radius:10px; margin:8px 0 0 0;">
            <span style="color:#249400;font-weight:700;">🎁 Bonus Progress:</span>
            <span style="color:#222;">{cycle_count}/70 deals for $1200 bonus</span>
            <div style='background:#e5e5e5;border-radius:8px;height:12px;margin-top:4px;'>
                <div style='background:#2dcc3a;width:{pct_to_bonus:.1f}%;height:12px;border-radius:8px;'></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    if bonus > 0:
        st.success(f"🎁 <b>Bonus:</b> ${bonus:,.0f} HIT!", icon="🎉")
    elif cycle_count >= 60:
        st.info(f"🚩 {cycle_count}/70 deals for $1200 bonus", icon="🎁")

    st.markdown("---")
    st.subheader("Recent Performance")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Today's Deals", daily_count)
    t2.metric("This Week", weekly_count)
    t3.metric("This Month", monthly_count)
    t4.metric("This Year", yearly_count)

    if prev_count > 0 and prev_start and prev_end:
        st.markdown("---")
        st.subheader("Previous Completed Cycle")
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("Deals", prev_count)
        p2.metric("Gross Payout", f"${prev_payout:,.2f}")
        p3.metric("Cycle", f"{prev_start} to {prev_end}")
        p4.metric("Pay Date", f"{prev_pay}")
        if net_paid is not None:
            st.markdown(
                f'<span style="font-weight:600;color:#107c10;">Net Payout (from FMO): ${net_paid:,.2f}</span>',
                unsafe_allow_html=True,
            )
        if paid_rows is not None and not paid_rows.empty:
            st.markdown("**Paid Policies in FMO Statement**")
            # Show only the main columns for paid policies, hide 'Agent' column
            show_cols = [col for col in paid_rows.columns if col.lower() not in ['agent']]
            st.dataframe(paid_rows[show_cols], use_container_width=True)

    st.markdown("---")
    st.markdown("#### All Deals in Current Cycle")
    if not deals_cycle.empty:
        st.dataframe(
            deals_cycle[['date_sold', 'carrier', 'product', 'policy_id']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No deals found in this commission cycle.")

    st.stop()














# =================== ADMIN DASHBOARD ===================
elif st.session_state.user_role.lower() == "admin":
    st.markdown(
        """
        <div style="padding:1.5em 1em 0.2em 1em; background: linear-gradient(90deg,#f5fff0,#eef5ff 80%); border-radius:16px;">
            <h1 style='font-size:2.3em; margin-bottom:0; color:#223969;'>
                🏆 HCS Commission <span style="color:#208b26;">Admin Dashboard</span>
            </h1>
        </div>
        """, unsafe_allow_html=True,
    )

    tabs = st.tabs([
    "🏆 Overview",
    "📋 Leaderboard",
    "📈 History",
    "📊 Live Counts",
    "⚙️ Settings",
    "📂 Clients",
    "💼 Vendor Pay",
    "🧾 Agent Net Pay",
    "📊 Vendor CPL/CPA"  # <-- This is tabs[8]
])

    # --- Smartly determine totals (if just uploaded, else pull last) ---
    if uploaded_file is not None and 'totals' in locals():
        _deals = int(totals["deals"])
        _agent_payout = totals["agent"]
        _owner_rev = totals["owner_rev"]
        _owner_profit = totals["owner_prof"]
    elif not history_df.empty:
        latest = history_df.iloc[-1]
        _deals = int(latest.total_deals)
        _agent_payout = latest.agent_payout
        _owner_rev = latest.owner_revenue
        _owner_profit = latest.owner_profit
    else:
        _deals = _agent_payout = _owner_rev = _owner_profit = 0

    # --- Overview Cards ---
    st.markdown("<div style='margin-top:1.5em;'></div>", unsafe_allow_html=True)
    o1, o2, o3, o4 = st.columns(4)
    o1.metric("Total Paid Deals", f"{_deals:,}")
    o2.metric("Agent Payout", f"${_agent_payout:,.2f}")
    o3.metric("Owner Revenue", f"${_owner_rev:,.2f}")
    o4.metric("Owner Profit", f"${_owner_profit:,.2f}")

    st.markdown("---")

    # --- Top Agents Leaderboard ---
    st.markdown("<h4 style='margin-bottom:0.3em;'>🥇 Top Agents This Month</h4>", unsafe_allow_html=True)
    if summary:
        df_led = pd.DataFrame(summary).sort_values("Paid Deals", ascending=False).head(6)
        st.dataframe(df_led.style.format({
            "Agent Payout": "${:,.2f}",
            "Owner Profit": "${:,.2f}"
        }), hide_index=True, use_container_width=True)
    else:
        st.info("Upload a statement to see leaderboard.")

    st.markdown("---")

    # --- Live Counts (Cards) ---
    st.markdown("<h4 style='margin-bottom:0.3em;'>📈 Live Deal Counts</h4>", unsafe_allow_html=True)
    try:
        df_api = fetch_all_today(limit=5000)
        df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
        today = pd.Timestamp.now(tz='US/Eastern').date()
        daily_mask = df_api["date_sold"].dt.date == today
        weekly_mask = df_api["date_sold"].dt.isocalendar().week == pd.Timestamp.now(tz='US/Eastern').isocalendar().week
        monthly_mask = df_api["date_sold"].dt.month == today.month

        lc1, lc2, lc3 = st.columns(3)
        lc1.metric("Today's Deals", len(df_api[daily_mask]))
        lc2.metric("This Week", len(df_api[weekly_mask]))
        lc3.metric("This Month", len(df_api[monthly_mask]))
    except Exception as e:
        st.warning("Live count data not available.")

    st.markdown("---")

    # --- Quickview (last 6 periods) ---
    st.markdown("<h4 style='margin-bottom:0.3em;'>📅 Recent Payroll Periods</h4>", unsafe_allow_html=True)
    if not history_df.empty:
        st.dataframe(
            history_df.tail(6)[
                ["upload_date", "total_deals", "agent_payout", "owner_revenue", "owner_profit"]
            ].rename(columns={
                "upload_date": "Date",
                "total_deals": "Deals",
                "agent_payout": "Agent Pay",
                "owner_revenue": "Owner Rev",
                "owner_profit": "Owner Profit"
            }).style.format({
                "Agent Pay": "${:,.2f}",
                "Owner Rev": "${:,.2f}",
                "Owner Profit": "${:,.2f}",
            }), use_container_width=True, hide_index=True
        )
    else:
        st.info("No payroll history yet.")
        


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
                    "Owner Profit": owner_prof,
                    "Net Paid": sub["Advance"].astype(float).sum()  # Add Net Paid from FMO
                })
                pdf_bytes = generate_agent_pdf(sub, agent)
                zf.writestr(f"{agent.replace(' ','_')}_Paystub.pdf", pdf_bytes)
            # Write admin summary CSV
            csv_buf = io.StringIO()
            w = csv.writer(csv_buf)
            w.writerow(["Agent","Paid Deals","Agent Payout","Owner Profit", "Net Paid"])
            for r in summary:
                w.writerow([r["Agent"], r["Paid Deals"], r["Agent Payout"], r["Owner Profit"], r["Net Paid"]])
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
           (latest.owner_revenue if not history_df.empty else 0))
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
        st.info("No history data yet.")
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

    # All vendor keys/code names and pretty display names
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

    # Assign rates to each vendor code that gets paid (expand as needed)
    VENDOR_RATES = {
        "francalls": 75,
        "hcsmedia": 75,
        "buffercall": 80,      # Aetna
        "acaking": 75,
        "raycalls": 75,
        # Add more here if you pay other vendors!
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
                "PaidPctNum": pct_paid,
                "Total Paid Amount": f"${paid_amt:,.2f}"
            })

        if vendor_summaries:
            df_sum = pd.DataFrame(vendor_summaries)
            st.subheader("Vendor Pay Summary Table")
            st.dataframe(df_sum.drop("PaidPctNum", axis=1), use_container_width=True)

            # ---- Grand Total Paid (bottom) ----
            total_paid = sum(
                float(str(row["Total Paid Amount"]).replace("$", "").replace(",", ""))
                for row in vendor_summaries
            )
            avg_paid_pct = (
                sum(row["PaidPctNum"] for row in vendor_summaries) / len(vendor_summaries)
                if vendor_summaries else 0
            )

            st.markdown(
                f"<div style='font-size:1.15em; margin-top:12px; color:#1a4301;'><b>Total Paid to All Vendors:</b> ${total_paid:,.2f}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:1.08em; margin-top:2px; color:#2a3647;'><b>Average Paid % Across Vendors:</b> {avg_paid_pct:.1f}%</div>",
                unsafe_allow_html=True,
            )

        # --- PDF GENERATOR with summary block at top ---
        def vendor_pdf(paid, unpaid, pretty, rate, pct_paid, paid_amt):
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
            pdf.cell(0, 8, fix(f"Summary:"), ln=True)
            pdf.set_font("Arial", "", 11)
            pdf.cell(0, 8, fix(f"Paid Deals: {paid_ct}"), ln=True)
            pdf.cell(0, 8, fix(f"Unpaid Deals: {unpaid_ct}"), ln=True)
            pdf.cell(0, 8, fix(f"Paid Percentage: {pct_paid:.1f}%"), ln=True)
            pdf.cell(0, 8, fix(f"Total Paid Amount: ${paid_amt:,.2f}"), ln=True)
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
                paid_ct = len(paid)
                unpaid_ct = len(unpaid)
                pct_paid = (paid_ct / (paid_ct + unpaid_ct) * 100) if (paid_ct + unpaid_ct) > 0 else 0
                paid_amt = paid_ct * rate
                pdf_bytes = vendor_pdf(paid, unpaid, pretty, rate, pct_paid, paid_amt)
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


with tabs[7]:
    st.header("🧾 Agent Net Pay (FMO Statement — HCS Tiers/Bonus)")
    fmo_file = st.file_uploader("Upload FMO Statement (.xlsx)", type=["xlsx"], key="agent_net_pay_fmo2")

    if fmo_file is not None:
        try:
            df_fmo = pd.read_excel(fmo_file, dtype=str)
            agent_col = "Agent"
            advance_col = next((c for c in df_fmo.columns if "advance" in c.lower()), None)
            if not advance_col or agent_col not in df_fmo.columns:
                st.error("Could not find Agent or Advance columns in this FMO file.")
                st.stop()

            # Only paid deals (Advance == 150)
            df_fmo[advance_col] = pd.to_numeric(df_fmo[advance_col], errors="coerce").fillna(0)
            paid_deals = df_fmo[df_fmo[advance_col] == 150]

            # Count paid deals per agent
            summary = paid_deals.groupby(agent_col).size().reset_index(name="Net Paid Deals")

            # Your commission model (apply tier logic for each agent)
            def calc_agent_payout(num_deals):
                if num_deals >= 200:
                    rate = 25
                elif num_deals >= 150:
                    rate = 22.5
                elif num_deals >= 120:
                    rate = 17.5
                else:
                    rate = 15
                bonus = 1200 if num_deals >= 70 else 0
                return num_deals * rate + bonus

            summary["Agent Net Payout"] = summary["Net Paid Deals"].apply(calc_agent_payout)
            summary["Agent Net Payout"] = summary["Agent Net Payout"].apply(lambda x: f"${x:,.2f}")

            st.dataframe(summary, use_container_width=True)

            st.download_button(
                "⬇️ Download CSV",
                summary.to_csv(index=False),
                file_name="agent_net_pay_summary.csv",
                mime="text/csv"
            )
            st.success("Showing all agents with Net Paid Deals (Advance == 150) and HCS payout model.")

        except Exception as e:
            st.error(f"Error processing FMO: {e}")

    else:
        st.info("Upload an FMO statement (.xlsx) to see net paid deals and payout by agent.")


with tabs[8]:
    st.header("📊 Vendor CPL/CPA Report (Vendor Style — Calls, Paid, CPA)")

    cpl_csv_file = st.file_uploader("Upload Vendor CPL (Calls/Leads) CSV", type=["csv"], key="vendor_cpl_tab8")
    fmo_file = st.file_uploader("Upload FMO Statement (xlsx)", type=["xlsx"], key="vendor_fmo_cpl_tab8")

    def normalize_name(x):
        return str(x).strip().lower()

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
    "hcs1p": "HCS1p",
    "hcsmediacpl": "HCS MEDIA CPL"
    # ...add more as needed
}
VENDOR_CPLS = {
    "acaking": 35,
    "joshaca": 30,
    "francalls": 25,
    "hcsmediacpl": 25,
    # ...add more as needed
}

    if cpl_csv_file and fmo_file:
        cpl_csv = pd.read_csv(cpl_csv_file, dtype=str)
        vendor_col = "list_list_description"
        first_name_col = "lead_first_name"
        last_name_col = "lead_last_name"
        if vendor_col not in cpl_csv.columns or first_name_col not in cpl_csv.columns or last_name_col not in cpl_csv.columns:
            st.error(f"CSV must have columns: '{vendor_col}', '{first_name_col}', and '{last_name_col}'.")
            st.write("CSV columns:", list(cpl_csv.columns))
            st.stop()
        cpl_csv['vendor_key'] = cpl_csv[vendor_col].astype(str).str.strip().str.lower().str.replace(' ', '')
        cpl_csv['first_name_norm'] = cpl_csv[first_name_col].astype(str).apply(normalize_name)
        cpl_csv['last_name_norm'] = cpl_csv[last_name_col].astype(str).apply(normalize_name)
        calls_by_vendor = cpl_csv.groupby('vendor_key').size().to_dict()

        # FMO: use columns "first_name", "last_name", "Advance"
        fmo = pd.read_excel(fmo_file, dtype=str)
        fmo_first_col = "first_name"
        fmo_last_col = "last_name"
        fmo_advance_col = "Advance"
        if fmo_first_col not in fmo.columns or fmo_last_col not in fmo.columns or fmo_advance_col not in fmo.columns:
            st.error("FMO XLSX missing one of the required columns: first_name, last_name, Advance.")
            st.write("FMO columns:", list(fmo.columns))
            st.stop()
        fmo['first_name_norm'] = fmo[fmo_first_col].astype(str).apply(normalize_name)
        fmo['last_name_norm'] = fmo[fmo_last_col].astype(str).apply(normalize_name)
        fmo['Advance'] = pd.to_numeric(fmo[fmo_advance_col], errors='coerce').fillna(0)
        paid_fmo = fmo[fmo['Advance'] > 0][['first_name_norm', 'last_name_norm']].drop_duplicates()

        cpl_stats = []
        for vkey, cpl in VENDOR_CPLS.items():
            pretty_name = VENDOR_CODES.get(vkey, vkey.upper())
            vendor_calls = cpl_csv[cpl_csv['vendor_key'] == vkey]
            calls_ct = vendor_calls.shape[0]
            # Merge by first + last name ONLY
            merged = pd.merge(
                vendor_calls[['first_name_norm','last_name_norm']],
                paid_fmo,
                on=['first_name_norm','last_name_norm'],
                how='inner'
            )
            paid_ct = merged.drop_duplicates().shape[0]
            vendor_cost = calls_ct * cpl
            paid_pct = (paid_ct / calls_ct * 100) if calls_ct > 0 else 0
            cpa_paid = (vendor_cost / paid_ct) if paid_ct else None
            cpl_stats.append({
                "Vendor": pretty_name,
                "CPL": f"${cpl:.2f}",
                "Total Calls (Leads)": calls_ct,
                "Paid Deals": paid_ct,
                "Paid %": f"{paid_pct:.1f}%",
                "Vendor Cost": f"${vendor_cost:,.2f}",
                "CPA (Paid)": f"${cpa_paid:,.2f}" if cpa_paid else "N/A",
            })

        df_cpl_stats = pd.DataFrame(cpl_stats)
        st.dataframe(df_cpl_stats, use_container_width=True)
        st.download_button(
            "⬇️ Download Vendor CPL/CPA Report (CSV)",
            df_cpl_stats.to_csv(index=False),
            file_name="vendor_cpl_cpa_report.csv",
            mime="text/csv"
        )
        st.info("Counts paid deals by matching (First+Last Name) between CPL and FMO (Advance > 0); CPA = Cost/paid matches. No retention.")

    else:
        st.warning("Upload both CPL (calls/leads) CSV and FMO Statement to see the CPL/CPA report.")
































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    



































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    


































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    






