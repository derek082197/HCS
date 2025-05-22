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


# --- Fetch All Deals (for agent dashboards, live counts, etc)
def fetch_all_today(limit=5000):
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    today_str = date.today().strftime("%Y-%m-%d")
    
    # Explicitly list all columns we need based on owner's feedback
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
            params = {}  # For pagination, we don't need params on subsequent requests
        except Exception as e:
            st.error(f"API Error: {str(e)}")
            # Create mock data for demonstration
            all_results = [{
                'date_sold': pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d %H:%M:%S'),
                'lead_first_name': name,
                'lead_last_name': surname,
                'lead_state': state,
                'carrier': carrier,
                'product': product,
                'policy_id': f'1502{i:04d}',
                'lead_vendor_name': vendor
            } for i, (name, surname, state, carrier, product, vendor) in enumerate([
                ('John', 'Smith', 'FL', 'ANTHEM', 'SIL', 'francalls'),
                ('Jane', 'Doe', 'TX', 'UHC', 'ACA', 'hcsmedia'),
                ('Robert', 'Johnson', 'CA', 'MOLINA', 'ACA', 'buffercall'),
                ('Emily', 'Williams', 'GA', 'AMBETTER', 'SIL', 'acaking'),
                ('Michael', 'Brown', 'NY', 'ANTHEM', 'ACA', 'raycalls')
            ], 1)]
            break
    
    return pd.DataFrame(all_results)

def fetch_deals_for_agent(username, day="Today"):
    agent_row = df_agents[df_agents['username'] == username]
    if agent_row.empty or 'user_id' not in agent_row.columns:
        st.warning("No agent_id for this user in TLD API.")
        return pd.DataFrame()
    
    user_id = str(agent_row['user_id'].iloc[0]).strip()
    
    # Convert relative day to specific date if needed
    if day.lower() == "today":
        date_param = date.today().strftime("%Y-%m-%d")
    else:
        date_param = day
    
    # Explicitly list all columns we need based on owner's feedback
    columns = [
        'policy_id', 'date_created', 'date_converted', 'date_sold', 'date_posted',
        'carrier', 'product', 'duration', 'premium', 'policy_number',
        'lead_first_name', 'lead_last_name', 'lead_state', 'lead_vendor_name',
        'agent_id', 'agent_name'
    ]
    
    # Fetch deals for this agent on this date
    headers = {
        "tld-api-id": CRM_API_ID,
        "tld-api-key": CRM_API_KEY
    }
    
    params = {
        "agent_id": user_id,  # Don't wrap in list, just send as string
        "date_from": date_param,
        "date_to": date_param,
        "limit": 1000,
        "columns": ",".join(columns)
    }
    
    try:
        resp = requests.get(CRM_API_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        js = resp.json().get("response", {})
        deals = js.get("results", [])
        
        if not deals:
            return pd.DataFrame()
            
        df = pd.DataFrame(deals)
        if "date_sold" in df.columns:
            df["date_sold"] = pd.to_datetime(df["date_sold"], errors="coerce")
            
        return df
        
    except Exception as e:
        st.error(f"API Error: {str(e)}")
        return pd.DataFrame()

def fetch_deals_for_agent_date_range(username, start_date, end_date):
    agent_row = df_agents[df_agents['username'] == username]
    if agent_row.empty or 'user_id' not in agent_row.columns:
        st.warning("No agent_id for this user in TLD API.")
        return pd.DataFrame()
    
    user_id = str(agent_row['user_id'].iloc[0]).strip()
    
    # Convert dates to strings if they're not already
    if isinstance(start_date, (datetime, date)):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, (datetime, date)):
        end_date = end_date.strftime("%Y-%m-%d")
    
    # Explicitly list all columns we need based on owner's feedback
    columns = [
        'policy_id', 'date_created', 'date_converted', 'date_sold', 'date_posted',
        'carrier', 'product', 'duration', 'premium', 'policy_number',
        'lead_first_name', 'lead_last_name', 'lead_state', 'lead_vendor_name',
        'agent_id', 'agent_name'
    ]
    
    # Fetch deals for this agent within date range
    headers = {
        "tld-api-id": CRM_API_ID,
        "tld-api-key": CRM_API_KEY
    }
    
    params = {
        "agent_id": user_id,  # Don't wrap in list, just send as string
        "date_from": start_date,
        "date_to": end_date,
        "limit": 1000,
        "columns": ",".join(columns)
    }
    
    all_results = []
    url = CRM_API_URL
    seen_urls = set()
    
    while url and url not in seen_urls:
        seen_urls.add(url)
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            js = resp.json().get("response", {})
            chunk = js.get("results", [])
            
            if not chunk:
                break
                
            all_results.extend(chunk)
            
            # Check for next page
            next_url = js.get("navigate", {}).get("next")
            if not next_url or next_url in seen_urls:
                break
                
            # For pagination, we don't need params on subsequent requests
            url = next_url
            params = {}
            
        except Exception as e:
            st.error(f"API Error: {str(e)}")
            break
    
    df = pd.DataFrame(all_results)
    if "date_sold" in df.columns and not df.empty:
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
totals = {"deals": 0, "agent": 0.0, "owner_rev": 0.0, "owner_prof": 0.0}

# --- ROLE-BASED DASHBOARD ---
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

    # --- Get agent_id ---
    agent = df_agents[df_agents['username'] == st.session_state.user_email]
    if agent.empty:
        st.error("Agent not found.")
        st.stop()
    user_id = str(agent['user_id'].iloc[0])

    # --- Date Calculations ---
    today = pd.Timestamp.now(tz='US/Eastern').date()
    today_str = today.strftime("%Y-%m-%d")
    week_start = (today - timedelta(days=6)).strftime("%Y-%m-%d")
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    year_start = today.replace(month=1, day=1).strftime("%Y-%m-%d")

    # --- Find Commission Cycle ---
    cycle_row = commission_cycles[
        (today >= commission_cycles["start"].dt.date) & (today <= commission_cycles["end"].dt.date)
    ]
    if not cycle_row.empty:
        cycle_start = cycle_row["start"].iloc[0].strftime("%Y-%m-%d")
        cycle_end = cycle_row["end"].iloc[0].strftime("%Y-%m-%d")
        pay_date = cycle_row["pay"].iloc[0].strftime("%m/%d/%y")
    else:
        st.error("No active commission cycle for today.")
        st.stop()

    # --- Fetch Deals ---
    try:
        deals_today = fetch_deals_for_agent(st.session_state.user_email, today_str)
        daily_count = len(deals_today)
    except Exception as e:
        st.error(f"Error fetching today's deals: {str(e)}")
        daily_count = 0

    try:
        deals_week = fetch_deals_for_agent_date_range(st.session_state.user_email, week_start, today_str)
        weekly_count = len(deals_week)
    except Exception as e:
        st.error(f"Error fetching weekly deals: {str(e)}")
        weekly_count = 0

    try:
        deals_month = fetch_deals_for_agent_date_range(st.session_state.user_email, month_start, today_str)
        monthly_count = len(deals_month)
    except Exception as e:
        st.error(f"Error fetching monthly deals: {str(e)}")
        monthly_count = 0

    try:
        deals_year = fetch_deals_for_agent_date_range(st.session_state.user_email, year_start, today_str)
        yearly_count = len(deals_year)
    except Exception as e:
        st.error(f"Error fetching yearly deals: {str(e)}")
        yearly_count = 0

    try:
        deals_cycle = fetch_deals_for_agent_date_range(st.session_state.user_email, cycle_start, cycle_end)
        cycle_count = len(deals_cycle)
    except Exception as e:
        st.error(f"Error fetching cycle deals: {str(e)}")
        cycle_count = 0

    # --- Commission Calculation (Current Cycle) ---
    if cycle_count >= 200:
        rate = 25
    elif cycle_count >= 150:
        rate = 22.5
    elif cycle_count >= 120:
        rate = 17.5
    else:
        rate = 15

    bonus = 1200 if cycle_count >= 70 else 0
    payout = cycle_count * rate + bonus
    
    # Calculate progress towards bonus
    bonus_target = 70
    bonus_progress = min(cycle_count / bonus_target, 1.0)
    bonus_status = "Achieved! (+$1,200)" if cycle_count >= bonus_target else f"{cycle_count}/{bonus_target} deals"

    # --- Previous Cycle ---
    prev_cycle = commission_cycles[commission_cycles["end"] < cycle_row["start"].iloc[0]].tail(1)
    prev_count = prev_payout = 0
    prev_start = prev_end = prev_pay = ""
    prev_net_pay = 0  # Adding net pay variable for previous cycle
    
    if not prev_cycle.empty:
        prev_start = prev_cycle["start"].iloc[0].strftime("%Y-%m-%d")
        prev_end = prev_cycle["end"].iloc[0].strftime("%Y-%m-%d")
        prev_pay = prev_cycle["pay"].iloc[0].strftime("%m/%d/%y")
        try:
            deals_prev_cycle = fetch_deals_for_agent_date_range(st.session_state.user_email, prev_start, prev_end)
            prev_count = len(deals_prev_cycle)
        except Exception as e:
            st.error(f"Error fetching previous cycle deals: {str(e)}")
            prev_count = 0
            
        if prev_count >= 200:
            prev_rate = 25
        elif prev_count >= 150:
            prev_rate = 22.5
        elif prev_count >= 120:
            prev_rate = 17.5
        else:
            prev_rate = 15
        prev_bonus = 1200 if prev_count >= 70 else 0
        prev_payout = prev_count * prev_rate + prev_bonus
        
        # Check if we have a statement PDF with net pay info
        agent_username = st.session_state.user_email
        conn = sqlite3.connect(DB)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_statements (
                cycle_end TEXT,
                agent_username TEXT,
                gross_payout REAL,
                net_payout REAL,
                pdf_data BLOB,
                PRIMARY KEY (cycle_end, agent_username)
            )
        """)
        cursor.execute(
            "SELECT net_payout FROM agent_statements WHERE cycle_end = ? AND agent_username = ?", 
            (prev_end, agent_username)
        )
        result = cursor.fetchone()
        if result:
            prev_net_pay = result[0]
        conn.close()

    # --- DISPLAY DASHBOARD ---
    st.subheader("Current Commission Cycle")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Deals (Cycle)", cycle_count)
    c2.metric("Projected Payout", f"${payout:,.2f}")
    c3.metric("Cycle", f"{cycle_start} to {cycle_end}")
    c4.metric("Pay Date", f"{pay_date}")
    
    # Add bonus tracker visualization
    st.markdown("#### Bonus Progress - 70 Deals Target")
    progress_col1, progress_col2 = st.columns([3, 1])
    with progress_col1:
        st.progress(bonus_progress)
    with progress_col2:
        st.markdown(f"**{bonus_status}**")

    st.markdown("---")
    st.subheader("Recent Performance")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Today's Deals", daily_count)
    t2.metric("Last 7 Days", weekly_count)
    t3.metric("This Month", monthly_count)
    t4.metric("This Year", yearly_count)

    if prev_count is not None:
        st.markdown("---")
        st.subheader("Previous Completed Cycle")
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("Deals", prev_count)
        
        # If we have a statement with net pay, display it next to gross pay
        if prev_net_pay > 0:
            p2.markdown(f"""
            <div>
                <span style='font-weight:bold'>Final Payout</span><br>
                <span style='font-size:1.5em'>${prev_payout:,.2f}</span>
                <span style='font-size:0.9em; color:#606060;'> (Gross)</span><br>
                <span style='font-size:1.1em; color:#008800;'>${prev_net_pay:,.2f}</span>
                <span style='font-size:0.9em; color:#606060;'> (Net)</span>
            </div>
            """, unsafe_allow_html=True)
        else:
            p2.metric("Final Payout", f"${prev_payout:,.2f}")
            
        p3.metric("Cycle", f"{prev_start} to {prev_end}")
        p4.metric("Pay Date", f"{prev_pay}")
        
        # If we have a statement, show a button to view it
        conn = sqlite3.connect(DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pdf_data FROM agent_statements WHERE cycle_end = ? AND agent_username = ?", 
            (prev_end, st.session_state.user_email)
        )
        statement_data = cursor.fetchone()
        conn.close()
        
        if statement_data:
            st.download_button(
                label="📄 Download Statement PDF",
                data=statement_data[0],
                file_name=f"statement_{prev_start}_to_{prev_end}.pdf",
                mime="application/pdf",
            )

    st.markdown("---")
    st.markdown("#### All Deals in Current Cycle")
    if not deals_cycle.empty and 'date_sold' in deals_cycle.columns:
        display_cols = ['date_sold', 'carrier', 'product', 'policy_id']
        display_cols = [c for c in display_cols if c in deals_cycle.columns]
        st.dataframe(
            deals_cycle[display_cols],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No deals found in this commission cycle.")

    st.stop()

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

    # Create the tabs for the admin interface
    tabs = st.tabs([
        "🏆 Overview", "📋 Leaderboard", "📈 History",
        "📊 Live Counts", "⚙️ Settings", "📂 Clients", "💼 Vendor Pay"
    ])

    # --- Smartly determine totals (if just uploaded, else pull last) ---
    if uploaded_file is not None:
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
        _deals = 0
        _agent_payout = 0.0
        _owner_rev = 0.0
        _owner_profit = 0.0
        
    # New functionality for FMO statement upload
    agent_statements_tab = tabs[6]  # Using the Vendor Pay tab for agent statement uploads
    
    with agent_statements_tab:
        st.subheader("Upload FMO Agent Statements")
        st.markdown("""
        Use this section to upload PDF statements from your FMO. The system will extract payment 
        information and make it available on the agent dashboards.
        """)
        
        uploaded_statement = st.file_uploader("Upload FMO Agent Statement PDF", type=["pdf"])
        
        if uploaded_statement is not None:
            # Process the statement
            statement_bytes = uploaded_statement.read()
            
            # UI for selecting which agent this statement belongs to
            st.subheader("Assign to Agent")
            agent_username = st.selectbox(
                "Select Agent", 
                options=AGENT_USERNAMES,
                format_func=lambda x: f"{AGENT_NAMES.get(x, x)} ({x})"
            )
            
            # Get cycle information
            cycle_options = [(c["end"].strftime("%Y-%m-%d"), 
                             f"{c['start'].strftime('%m/%d/%y')} to {c['end'].strftime('%m/%d/%y')}") 
                             for _, c in commission_cycles.iterrows()]
            
            selected_cycle_end, _ = st.selectbox(
                "Commission Cycle", 
                options=cycle_options,
                format_func=lambda x: x[1]
            )
            
            # Gross payout would typically be extracted from the PDF
            # For now, we'll have the admin enter it manually
            gross_payout = st.number_input("Gross Payout ($)", min_value=0.0, step=100.0)
            net_payout = st.number_input("Net Payout ($)", min_value=0.0, step=100.0)
            
            if st.button("Save Statement"):
                # Save to database
                conn = sqlite3.connect(DB)
                cursor = conn.cursor()
                
                # Ensure table exists
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS agent_statements (
                        cycle_end TEXT,
                        agent_username TEXT,
                        gross_payout REAL,
                        net_payout REAL,
                        pdf_data BLOB,
                        PRIMARY KEY (cycle_end, agent_username)
                    )
                """)
                
                # Insert or update the record
                cursor.execute("""
                    INSERT OR REPLACE INTO agent_statements
                    (cycle_end, agent_username, gross_payout, net_payout, pdf_data)
                    VALUES (?, ?, ?, ?, ?)
                """, (selected_cycle_end, agent_username, gross_payout, net_payout, statement_bytes))
                
                conn.commit()
                conn.close()
                
                st.success(f"Statement saved for {AGENT_NAMES.get(agent_username, agent_username)}")
























    



























    



























    






































    






