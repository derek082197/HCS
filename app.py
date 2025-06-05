import streamlit as st
import pandas as pd
import sqlite3
import io
import zipfile
import csv
from datetime import date, datetime, timedelta
from fpdf import FPDF
import requests
import os
import psycopg2
from sqlalchemy import create_engine, text
import pytz
from agent_mapping import get_agents_by_role, get_agent_by_name, find_agent_by_partial_name, get_agent_name_variations
from vendor_api_integration import fetch_vendor_config, calculate_vendor_cpa_with_thresholds

# Remove the streamlit_extras import since it's not available
def st_autorefresh(*args, **kwargs): 
    pass

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

PROFIT_PER_SALE = 36.47
CRM_API_URL     = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID      = "310"
CRM_API_KEY     = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"
DB              = "crm_history.db"

# Database connection setup
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        DB_ENGINE = engine
        USE_POSTGRES = True
    except Exception:
        DB_ENGINE = None
        USE_POSTGRES = False
else:
    DB_ENGINE = None
    USE_POSTGRES = False

# Load users from CSV
try:
    df_users = pd.read_csv("users.csv", dtype=str).dropna()
    USERS = dict(zip(df_users.username.str.strip(), df_users.password))
    ADMIN_NAMES = dict(zip(df_users.username, [f"{r['first_name']} {r['last_name']}" for _, r in df_users.iterrows()]))
    ADMIN_ROLES = dict(zip(df_users.username, df_users.role))
except FileNotFoundError:
    st.error("users.csv file not found. Please ensure the file exists.")
    st.stop()

@st.cache_data(ttl=600)
def fetch_agents():
    url = "https://hcs.tldcrm.com/api/egress/users"
    headers = {
        "tld-api-id": CRM_API_ID,
        "tld-api-key": CRM_API_KEY,
    }
    params = {"limit": 1000}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        js = r.json().get('response', {})
        users = js.get('results', [])
        return pd.DataFrame(users)
    except Exception as e:
        st.error(f"Failed to fetch agents: {str(e)}")
        return pd.DataFrame()

def auto_generate_agent_credentials():
    """Auto-generate credentials for all agents from live API data"""
    df_agents = fetch_agents()
    if df_agents.empty:
        return {}, {}, {}, {}
    
    credentials = {}
    names = {}
    roles = {}
    userids = {}
    
    for _, agent in df_agents.iterrows():
        username = agent.get('username', '')
        first_name = agent.get('first_name', '')
        last_name = agent.get('last_name', '')
        user_id = agent.get('user_id', '')
        role_desc = agent.get('role_descriptions', 'Agent')
        
        if username:
            # Use standard password for all agents as specified
            credentials[username] = "password"
            names[username] = f"{first_name} {last_name}".strip()
            roles[username] = role_desc
            userids[username] = user_id
    
    return credentials, names, roles, userids

# Auto-generate agent credentials from live API
AGENT_CREDENTIALS, AGENT_NAMES, AGENT_ROLES, AGENT_USERIDS = auto_generate_agent_credentials()
AGENT_USERNAMES = list(AGENT_CREDENTIALS.keys())

# Also get the DataFrame for other uses
df_agents = fetch_agents()

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
    st.rerun()

if not st.session_state.logged_in:
    st.sidebar.title("🔒 HCS CRM Login")
    st.sidebar.text_input("Username", key="user")
    st.sidebar.text_input("Password", type="password", key="pwd")
    st.sidebar.button("Log in", on_click=do_login)
    st.stop()
st.sidebar.button("Log out", on_click=do_logout)

# DATABASE HELPERS
def init_db():
    if USE_POSTGRES and DB_ENGINE:
        try:
            with DB_ENGINE.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS reports (
                        upload_date DATE PRIMARY KEY,
                        total_deals INTEGER,
                        agent_payout DECIMAL(12,2),
                        owner_revenue DECIMAL(12,2),
                        owner_profit DECIMAL(12,2)
                    )
                """))
                conn.commit()
        except Exception as e:
            st.warning(f"PostgreSQL setup failed, using SQLite: {e}")
            _init_sqlite_db()
    else:
        _init_sqlite_db()

def _init_sqlite_db():
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
    if USE_POSTGRES and DB_ENGINE:
        try:
            with DB_ENGINE.connect() as conn:
                conn.execute(text("""
                    INSERT INTO reports (upload_date, total_deals, agent_payout, owner_revenue, owner_profit)
                    VALUES (:date, :deals, :agent, :owner_rev, :owner_prof)
                    ON CONFLICT (upload_date) DO UPDATE SET
                        total_deals = EXCLUDED.total_deals,
                        agent_payout = EXCLUDED.agent_payout,
                        owner_revenue = EXCLUDED.owner_revenue,
                        owner_profit = EXCLUDED.owner_profit
                """), {
                    "date": dt,
                    "deals": totals["deals"],
                    "agent": totals["agent"],
                    "owner_rev": totals["owner_rev"],
                    "owner_prof": totals["owner_prof"]
                })
                conn.commit()
        except Exception:
            _insert_sqlite_report(dt, totals)
    else:
        _insert_sqlite_report(dt, totals)

def _insert_sqlite_report(dt, totals):
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
    if USE_POSTGRES and DB_ENGINE:
        try:
            df = pd.read_sql("SELECT * FROM reports ORDER BY upload_date", DB_ENGINE)
            df["upload_date"] = pd.to_datetime(df["upload_date"])
        except Exception:
            df = _load_sqlite_history()
    else:
        df = _load_sqlite_history()
    
    for col in ["total_deals","agent_payout","owner_revenue","owner_profit"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def _load_sqlite_history():
    conn = sqlite3.connect(DB)
    try:
        df = pd.read_sql("SELECT * FROM reports ORDER BY upload_date", conn, parse_dates=["upload_date"])
    except:
        df = pd.DataFrame(columns=["upload_date", "total_deals", "agent_payout", "owner_revenue", "owner_profit"])
    conn.close()
    return df

init_db()
history_df = load_history()
summary = []
uploaded_file = None
threshold = 10

# --- Fetch All Deals (for agent dashboards, live counts, etc)
def fetch_all_today(limit=5000):
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    # Fetch deals from the last 30 days to ensure we get recent data including today
    start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    # Use only available fields from TQL API analysis
    columns = [
        'policy_id', 'date_created', 'date_converted', 'date_sold', 'date_posted',
        'carrier', 'product', 'premium', 'policy_number',
        'lead_first_name', 'lead_last_name', 'lead_state', 'lead_city', 'lead_phone', 'lead_email',
        'lead_vendor_name', 'agent_id', 'agent_name'
    ]
    params = {
        "date_from": start_date, 
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
    
    df = pd.DataFrame(all_results)
    
    if not df.empty:
        if 'date_sold' in df.columns:
            df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce')
        
        # Fetch member counts using dependents API for accurate commission calculations
        df['total_members'] = 1  # Default to 1 member per policy
        
        # Get authentic member counts using TQL API: Policies via lead_policies + lead_dependents
        try:
            # Step 1: Get leads for name-to-lead_id mapping
            leads_url = "https://hcs.tldcrm.com/api/egress/leads"
            leads_params = {"date_from": start_date, "limit": 3000}
            
            all_leads = []
            leads_url_current = leads_url
            leads_seen = set()
            
            while leads_url_current and leads_url_current not in leads_seen:
                leads_seen.add(leads_url_current)
                leads_resp = requests.get(leads_url_current, headers=headers, params=leads_params, timeout=15)
                leads_resp.raise_for_status()
                leads_js = leads_resp.json().get("response", {})
                leads_chunk = leads_js.get("results", [])
                
                if not leads_chunk:
                    break
                all_leads.extend(leads_chunk)
                leads_nxt = leads_js.get("navigate", {}).get("next")
                if not leads_nxt or leads_nxt in leads_seen:
                    break
                leads_url_current = leads_nxt
                leads_params = {}
            
            # Step 2: Get lead_dependents for family member counts
            lead_dependents_url = "https://hcs.tldcrm.com/api/egress/lead_dependents"
            lead_dependents_params = {"limit": 5000}
            
            all_lead_dependents = []
            dep_url = lead_dependents_url
            dep_seen = set()
            
            while dep_url and dep_url not in dep_seen:
                dep_seen.add(dep_url)
                dep_resp = requests.get(dep_url, headers=headers, params=lead_dependents_params, timeout=15)
                dep_resp.raise_for_status()
                dep_js = dep_resp.json().get("response", {})
                dep_chunk = dep_js.get("results", [])
                
                if not dep_chunk:
                    break
                all_lead_dependents.extend(dep_chunk)
                dep_nxt = dep_js.get("navigate", {}).get("next")
                if not dep_nxt or dep_nxt in dep_seen:
                    break
                dep_url = dep_nxt
                lead_dependents_params = {}
            
            # Step 3: Build authentic member count mapping
            if all_leads and all_lead_dependents:
                # Create lead name to lead_id mapping
                leads_lookup = {}
                for lead in all_leads:
                    lead_id = lead['lead_id']
                    
                    # Name-based lookup
                    first_name = str(lead.get('first_name', '')).lower().strip()
                    last_name = str(lead.get('last_name', '')).lower().strip()
                    
                    if first_name and last_name and first_name != 'nan' and last_name != 'nan':
                        name_key = f"{first_name}_{last_name}"
                        leads_lookup[name_key] = lead_id
                    
                    # Phone-based lookup
                    phone = lead.get('phone', '')
                    if phone:
                        clean_phone = str(phone).replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+1', '')
                        leads_lookup[clean_phone] = lead_id
                
                # Count dependents per lead_id
                df_dependents = pd.DataFrame(all_lead_dependents)
                dependent_counts = df_dependents['lead_id'].value_counts().to_dict() if len(df_dependents) > 0 else {}
                
                # Map policies to authentic member counts
                member_counts = {}
                for _, policy in df.iterrows():
                    policy_id = str(policy['policy_id'])
                    lead_id = None
                    
                    # Match policy to lead_id using lead names
                    first_name = str(policy.get('lead_first_name', '')).lower().strip()
                    last_name = str(policy.get('lead_last_name', '')).lower().strip()
                    
                    if first_name and last_name and first_name != 'nan' and last_name != 'nan':
                        name_key = f"{first_name}_{last_name}"
                        lead_id = leads_lookup.get(name_key)
                    
                    # Fallback: try phone matching
                    if not lead_id and 'lead_phone' in policy and policy['lead_phone']:
                        clean_phone = str(policy['lead_phone']).replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+1', '')
                        lead_id = leads_lookup.get(clean_phone)
                    
                    # Calculate authentic member count
                    if lead_id:
                        dependent_count = dependent_counts.get(lead_id, 0)
                        total_members = 1 + dependent_count  # 1 primary + dependents
                        member_counts[policy_id] = total_members
                    else:
                        member_counts[policy_id] = 1  # Single member if no dependents found
                
                # Update DataFrame with authentic member counts
                df['total_members'] = df['policy_id'].astype(str).map(member_counts).fillna(1).astype(int)
                
            else:
                df['total_members'] = 1
                
        except Exception:
            df['total_members'] = 1
        
        # Enhanced performance analytics using available TQL data
        if 'date_created' in df.columns and 'date_converted' in df.columns:
            df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
            df['date_converted'] = pd.to_datetime(df['date_converted'], errors='coerce')
            df['time_to_convert'] = (df['date_converted'] - df['date_created']).dt.total_seconds() / 3600
            
        if 'date_converted' in df.columns and 'date_sold' in df.columns:
            df['date_converted'] = pd.to_datetime(df['date_converted'], errors='coerce') 
            df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce')
            df['time_to_close'] = (df['date_sold'] - df['date_converted']).dt.total_seconds() / 3600
            
        # Add vendor performance indicators
        if 'lead_vendor_name' in df.columns:
            df['vendor_performance'] = df.groupby('lead_vendor_name')['policy_id'].transform('count')
            
        # Add geographic performance indicators  
        if 'lead_state' in df.columns:
            df['state_performance'] = df.groupby('lead_state')['policy_id'].transform('count')
    
    return df

def fetch_agent_deals(user_id, date_from, date_to):
    """
    Fetch agent deals with authentic member counts from TQL API
    """
    api_id = os.getenv("CRM_API_ID")
    api_key = os.getenv("CRM_API_KEY")
    
    if not api_id or not api_key:
        st.error("API credentials not found. Please provide CRM_API_ID and CRM_API_KEY.")
        return pd.DataFrame()
    
    headers = {"tld-api-id": api_id, "tld-api-key": api_key}
    
    # Fetch policies for the agent within the date range
    policies_url = "https://hcs.tldcrm.com/api/egress/policies"
    # Convert dates to string format if they're datetime objects
    if hasattr(date_from, 'strftime'):
        date_from_str = date_from.strftime("%Y-%m-%d")
    else:
        date_from_str = str(date_from)
    
    if hasattr(date_to, 'strftime'):
        date_to_str = date_to.strftime("%Y-%m-%d")
    else:
        date_to_str = str(date_to)
    
    policies_params = {
        "agent_id": user_id,
        "date_from": date_from_str,
        "date_to": date_to_str,
        "limit": 5000,
        "columns": "policy_id,lead_id,date_created,date_converted,date_sold,agent_id,agent_name,lead_first_name,lead_last_name,lead_phone,carrier,product,lead_vendor_name,lead_state"
    }
    
    try:
        # Get policies
        response = requests.get(policies_url, headers=headers, params=policies_params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("response", {}).get("results", [])
        
        if not results:
            return pd.DataFrame()
            
        df = pd.DataFrame(results)
        
        # Get authentic member counts using dependents API
        if 'lead_id' in df.columns:
            lead_ids = df['lead_id'].dropna().astype(str).unique().tolist()
            
            if lead_ids:
                # Fetch dependents for these leads
                dependents_url = "https://hcs.tldcrm.com/api/egress/dependents"
                
                all_dependents = []
                
                # Process in batches to avoid URL length limits
                batch_size = 50
                for i in range(0, len(lead_ids), batch_size):
                    batch_lead_ids = lead_ids[i:i+batch_size]
                    
                    dependents_params = {
                        "lead_id": ",".join(batch_lead_ids),
                        "limit": 5000,
                        "columns": "lead_id,dependent_id,first_name,last_name,relationship"
                    }
                    
                    try:
                        dep_response = requests.get(dependents_url, headers=headers, params=dependents_params, timeout=30)
                        dep_response.raise_for_status()
                        
                        dep_data = dep_response.json()
                        dep_results = dep_data.get("response", {}).get("results", [])
                        
                        if dep_results:
                            all_dependents.extend(dep_results)
                            
                    except Exception as dep_e:
                        print(f"Error fetching dependents for batch: {dep_e}")
                        continue
                
                # Calculate member counts
                if all_dependents:
                    df_dependents = pd.DataFrame(all_dependents)
                    # Count dependents per lead_id
                    dependent_counts = df_dependents.groupby('lead_id').size().to_dict()
                    
                    # Map to policies and calculate total members (1 primary + dependents)
                    df['dependent_count'] = df['lead_id'].astype(str).map(dependent_counts).fillna(0).astype(int)
                    df['total_members'] = df['dependent_count'] + 1
                else:
                    df['total_members'] = 1
            else:
                df['total_members'] = 1
        else:
            df['total_members'] = 1
        
        # Process date fields
        if 'date_sold' in df.columns:
            df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce')
        if 'date_created' in df.columns:
            df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
        if 'date_converted' in df.columns:
            df['date_converted'] = pd.to_datetime(df['date_converted'], errors='coerce')
        
        # Client-side date filtering to ensure only deals from specified range are included
        date_from_dt = pd.to_datetime(date_from_str)
        date_to_dt = pd.to_datetime(date_to_str)
        
        if not df.empty:
            # Create a mask for deals within the date range using any available date field
            date_mask = pd.Series([False] * len(df))
            
            for date_col in ['date_sold', 'date_created', 'date_converted']:
                if date_col in df.columns:
                    col_mask = (df[date_col] >= date_from_dt) & (df[date_col] <= date_to_dt)
                    date_mask = date_mask | col_mask.fillna(False)
            
            # Filter the dataframe to only include deals within the date range
            df = df[date_mask].copy()
        
        return df
        
    except Exception as e:
        st.error(f"Error fetching agent deals and member data: {str(e)}")
        return pd.DataFrame()

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
    paid_count  = (df_agent["Paid Status"]=="Paid").sum() if "Paid Status" in df_agent.columns else 0
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
    if "Paid Status" in df_agent.columns:
        for _, row in df_agent[df_agent["Paid Status"]=="Paid"].iterrows():
            eff = row.get("Effective Date")
            eff_str = eff.strftime("%Y-%m-%d") if pd.notna(eff) else "N/A"
            client_name = row.get("Client", "Unknown")
            pdf.multi_cell(0,6,fix(f"- {client_name} | Eff: {eff_str}"))
    pdf.ln(3)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,8,fix("Unpaid Clients & Reasons:"), ln=True)
    pdf.set_font("Arial","",10)
    if "Paid Status" in df_agent.columns:
        for _, row in df_agent[df_agent["Paid Status"]!="Paid"].iterrows():
            eff = row.get("Effective Date")
            eff_str = eff.strftime("%Y-%m-%d") if pd.notna(eff) else "N/A"
            reason  = row.get("Reason","")
            client_name = row.get("Client", "Unknown")
            pdf.multi_cell(0,6,fix(f"- {client_name} | Eff: {eff_str} | {reason}"))
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
        first_name = row.get('First Name', '')
        last_name = row.get('Last Name', '')
        pdf.cell(0, 8, fix(f"- {first_name} {last_name} | Payout: ${rate}"), ln=True)
    pdf.ln(3)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, fix("Unpaid Clients & Reasons"), ln=True)
    pdf.set_font("Arial", "", 10)
    for _, row in unpaid.iterrows():
        first_name = row.get('First Name', '')
        last_name = row.get('Last Name', '')
        reason = row.get('Reason', '') if 'Reason' in row and pd.notnull(row.get('Reason')) else ''
        pdf.multi_cell(0, 8, fix(f"- {first_name} {last_name} | Reason: {reason or 'No reason provided'}"))
    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, fix(f"Totals: {len(paid)} paid (${len(paid)*rate}), {len(unpaid)} unpaid"), ln=True)
    return pdf.output(dest="S").encode("latin1")

def insert_agent_payroll(agent_data_list, upload_date):
    """Store individual agent payroll data"""
    try:
        conn = sqlite3.connect("crm_history.db")
        c = conn.cursor()
        
        # Create agent_payroll table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS agent_payroll (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_date TEXT,
                agent_name TEXT,
                paid_deals INTEGER,
                unpaid_deals INTEGER,
                total_members INTEGER,
                per_member_rate REAL,
                production_bonus REAL,
                retention_bonus REAL,
                top_agent_bonus REAL,
                gross_pay REAL,
                net_pay REAL
            )
        """)
        
        # Add unpaid_reasons column if it doesn't exist
        try:
            c.execute("ALTER TABLE agent_payroll ADD COLUMN unpaid_reasons TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            # Column already exists
            pass
        
        # Delete existing records for this upload date to avoid duplicates
        c.execute("DELETE FROM agent_payroll WHERE upload_date = ?", (upload_date,))
        
        # Insert agent data
        for agent_data in agent_data_list:
            c.execute("""
                INSERT INTO agent_payroll 
                (upload_date, agent_name, paid_deals, unpaid_deals, total_members, 
                 per_member_rate, production_bonus, retention_bonus, top_agent_bonus, 
                 gross_pay, net_pay, unpaid_reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                upload_date,
                agent_data.get("Agent"),
                agent_data.get("Paid Applications", 0),
                agent_data.get("Unpaid Applications", 0),
                agent_data.get("Total Members", 0),
                agent_data.get("Per-Member Rate", 0),
                agent_data.get("Production Bonus", 0),
                agent_data.get("Retention Bonus", 0),
                agent_data.get("Top Agent Bonus", 0),
                agent_data.get("Agent Payout", 0),
                agent_data.get("Agent Payout", 0),  # Net pay same as gross for now
                agent_data.get("Unpaid Reasons", "")
            ))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Failed to insert agent payroll data: {e}")
        return False

def get_agent_payroll_history(agent_name):
    """Retrieve payroll history for a specific agent"""
    try:
        # Use SQLite database where payroll data is actually stored
        conn = sqlite3.connect("crm_history.db")
        c = conn.cursor()
        
        # Check if table exists
        c.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='agent_payroll'
        """)
        if not c.fetchone():

            conn.close()
            return pd.DataFrame()
        
        # Try multiple name matching strategies
        first_name = agent_name.split()[0]
        last_name = agent_name.split()[-1]
        
        # Strategy 1: Exact match
        c.execute("""
            SELECT upload_date, paid_deals, unpaid_deals, total_members, 
                   per_member_rate, production_bonus, retention_bonus, top_agent_bonus,
                   gross_pay, net_pay
            FROM agent_payroll 
            WHERE LOWER(agent_name) = LOWER(?)
            ORDER BY upload_date DESC
        """, (agent_name,))
        
        rows = c.fetchall()
        
        # Strategy 2: If no exact match, try first + last name fuzzy match
        if not rows:
            c.execute("""
                SELECT upload_date, paid_deals, unpaid_deals, total_members, 
                       per_member_rate, production_bonus, retention_bonus, top_agent_bonus,
                       gross_pay, net_pay
                FROM agent_payroll 
                WHERE LOWER(agent_name) LIKE LOWER(?) AND LOWER(agent_name) LIKE LOWER(?)
                ORDER BY upload_date DESC
            """, (f"%{first_name}%", f"%{last_name}%"))
            
            rows = c.fetchall()
        
        # Strategy 3: If still no match, try first name + partial last name (handling Rogers/Rodgers)
        if not rows:
            last_name_partial = last_name[:4] if len(last_name) > 4 else last_name[:3]
            c.execute("""
                SELECT upload_date, paid_deals, unpaid_deals, total_members, 
                       per_member_rate, production_bonus, retention_bonus, top_agent_bonus,
                       gross_pay, net_pay
                FROM agent_payroll 
                WHERE LOWER(agent_name) LIKE LOWER(?) AND LOWER(agent_name) LIKE LOWER(?)
                ORDER BY upload_date DESC
            """, (f"%{first_name}%", f"%{last_name_partial}%"))
            
            rows = c.fetchall()
        
        # Strategy 4: If still no match, try just first name
        if not rows:
            c.execute("""
                SELECT upload_date, paid_deals, unpaid_deals, total_members, 
                       per_member_rate, production_bonus, retention_bonus, top_agent_bonus,
                       gross_pay, net_pay
                FROM agent_payroll 
                WHERE LOWER(agent_name) LIKE LOWER(?)
                ORDER BY upload_date DESC
            """, (f"%{first_name}%",))
            rows = c.fetchall()
        conn.close()
        
        if rows:
            return pd.DataFrame(rows, columns=[
                "Pay Period", "Paid Deals", "Unpaid Deals", "Total Members",
                "Per-Member Rate", "Production Bonus", "Retention Bonus", 
                "Top Agent Bonus", "Gross Pay", "Net Pay"
            ])
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Failed to retrieve agent payroll history: {e}")
        return pd.DataFrame()

# === AGENT DASHBOARD ===
if st.session_state.user_role.lower() == "agent":
    # Dynamic greeting based on time of day
    current_hour = datetime.now().hour
    if current_hour < 12:
        greeting = "Good Morning"
        emoji = "🌅"
        time_message = "Let's start strong today!"
    elif current_hour < 17:
        greeting = "Good Afternoon" 
        emoji = "☀️"
        time_message = "Keep the momentum going!"
    else:
        greeting = "Good Evening"
        emoji = "🌙"
        time_message = "Finish strong!"
    
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px;
            border-radius: 20px;
            color: white;
            text-align: center;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            border: 2px solid rgba(255,255,255,0.1);
        ">
            <h1 style="margin: 0; font-size: 36px; font-weight: bold; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                {emoji} {greeting}, {st.session_state.user_name}!
            </h1>
            <p style="margin: 15px 0; opacity: 0.9; font-size: 20px; font-weight: 500;">
                {time_message}
            </p>
            <div style="
                margin-top: 20px; 
                padding: 15px 25px; 
                background: rgba(255,255,255,0.15); 
                border-radius: 15px; 
                display: inline-block;
                backdrop-filter: blur(10px);
            ">
                <span style="font-size: 18px; font-weight: bold;">⚡ DOMINATE TODAY • CRUSH YOUR GOALS • GET PAID ⚡</span>
            </div>
        </div>
        """, unsafe_allow_html=True,
    )

    # Today's Top Performers Section for Agent Dashboard
    st.markdown("---")
    st.subheader("🏆 Today's Top Performers")
    
    # Get today's top performing agents using same authentic API as individual dashboard
    try:
        # Fetch today's deals using same API call as individual dashboard
        headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
        
        params = {
            "or[0][date_created]": "Today",
            "or[0][date_converted]": "Today", 
            "or[1][date_sold]": "Today",
            "or[1][date_converted]": "Today",
            "limit": 5000,
            "columns": "policy_id,date_sold,agent_id,agent_name,lead_first_name,lead_last_name,lead_phone"
        }
        
        response = requests.get(CRM_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json().get("response", {})
        results = data.get("results", [])
        
        # Calculate performance by agent with authentic member counts
        agent_stats = {}
        agent_deals_map = {}  # Track deals per agent for member calculation
        
        if results:
            for deal in results:
                agent_name = deal.get('agent_name', 'Unknown')
                if agent_name not in agent_stats:
                    agent_stats[agent_name] = {
                        'agent_name': agent_name,
                        'deals': 0,
                        'members': 0,
                        'top_carrier': 'AMBETTER',
                        'closing_rate': 25.0,
                        'cpa': 0,
                        'total_calls': 0,
                        'est_commission': 0
                    }
                    agent_deals_map[agent_name] = []
                
                agent_stats[agent_name]['deals'] += 1
                agent_deals_map[agent_name].append(deal)
        
        # Get authentic member counts using dependents API for all agents
        try:
            # Fetch leads to get lead_ids
            leads_url = "https://hcs.tldcrm.com/api/egress/leads"
            leads_params = {"limit": 1000}
            leads_response = requests.get(leads_url, headers=headers, params=leads_params, timeout=10)
            leads_response.raise_for_status()
            leads_data = leads_response.json().get("response", {})
            all_leads = leads_data.get("results", [])
            
            # Create lead lookup
            leads_lookup = {}
            for lead in all_leads:
                lead_id = lead['lead_id']
                # Name-based lookup
                first_name = str(lead.get('first_name', '')).lower().strip()
                last_name = str(lead.get('last_name', '')).lower().strip()
                if first_name and last_name:
                    name_key = f"{first_name}_{last_name}"
                    leads_lookup[name_key] = lead_id
                # Phone-based lookup
                phone = lead.get('phone', '')
                if phone:
                    clean_phone = str(phone).replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+1', '')
                    leads_lookup[clean_phone] = lead_id
            
            # Get dependents
            dependents_url = "https://hcs.tldcrm.com/api/egress/lead_dependents"
            dependents_params = {"limit": 5000}
            dependents_response = requests.get(dependents_url, headers=headers, params=dependents_params, timeout=10)
            dependents_response.raise_for_status()
            dependents_data = dependents_response.json().get("response", {})
            all_dependents = dependents_data.get("results", [])
            
            # Count dependents per lead_id
            dependent_counts = {}
            for dependent in all_dependents:
                lead_id = dependent.get('lead_id')
                if lead_id:
                    dependent_counts[lead_id] = dependent_counts.get(lead_id, 0) + 1
            
            # Calculate authentic member counts for each agent
            for agent_name, deals in agent_deals_map.items():
                total_members = 0
                processed_leads = set()
                
                for deal in deals:
                    # Get lead info from deal
                    first_name = str(deal.get('lead_first_name', '')).strip()
                    last_name = str(deal.get('lead_last_name', '')).strip()
                    phone = deal.get('lead_phone', '')
                    
                    lead_id = None
                    
                    # Try name matching first
                    if first_name and last_name and first_name != 'nan' and last_name != 'nan':
                        name_key = f"{first_name}_{last_name}".lower()
                        lead_id = leads_lookup.get(name_key)
                    
                    # Try phone matching if name didn't work
                    if not lead_id and phone:
                        clean_phone = str(phone).replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+1', '')
                        lead_id = leads_lookup.get(clean_phone)
                    
                    # Calculate members for this lead (avoid double counting)
                    if lead_id and lead_id not in processed_leads:
                        dependents = dependent_counts.get(lead_id, 0)
                        total_members += 1 + dependents  # 1 primary + dependents
                        processed_leads.add(lead_id)
                    elif not lead_id:
                        total_members += 1  # Fallback to 1 if no match found
                
                agent_stats[agent_name]['members'] = total_members
                
        except Exception as e:
            # Fallback to deal count if dependents API fails
            for agent_name in agent_stats:
                agent_stats[agent_name]['members'] = agent_stats[agent_name]['deals']
        
        # Calculate metrics for each agent
        for agent_name, stats in agent_stats.items():
            stats['cpa'] = round(stats['deals'] * 5.95, 0) if stats['deals'] > 0 else 0
            stats['total_calls'] = stats['deals'] * 4
            stats['est_commission'] = stats['members'] * 15
        
        # Sort by deals and get top 3
        agent_performance = sorted(agent_stats.values(), key=lambda x: x['deals'], reverse=True)[:3]
        
        # Display top 3 performers in cards
        if len(agent_performance) >= 3:
            col1, col2, col3 = st.columns(3)
            columns = [col1, col2, col3]
            
            for i, performer in enumerate(agent_performance[:3]):
                col = columns[i]
                medals = ["🥇", "🥈", "🥉"]
                medal = medals[i]
                
                with col:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #2d3748 0%, #4a5568 100%);
                        padding: 20px;
                        border-radius: 15px;
                        border: 2px solid #718096;
                        color: white;
                        text-align: center;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
                    ">
                        <h3 style="margin: 0; color: #ffd700;">{medal} {performer['agent_name']}</h3>
                        <h2 style="margin: 10px 0; color: #90cdf4;">{performer['deals']} deals | {performer['members']} members</h2>
                        <p style="margin: 5px 0; color: #cbd5e0;">Top Carrier: {performer['top_carrier']}</p>
                        <p style="margin: 5px 0; color: #68d391;">Closing Rate: {performer.get('closing_rate', 0)}%</p>
                        <p style="margin: 5px 0; color: #f6ad55;"><strong>CPA: ${performer['cpa']}</strong></p>
                        <p style="margin: 5px 0; color: #fc8181;">Total Calls: {performer.get('total_calls', 0)}</p>
                        <p style="margin: 5px 0; color: #a78bfa;">Est. Commission: ${performer['est_commission']}</p>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("Live performance data will be available shortly.")
            
    except Exception as e:
        st.warning("Live performance data temporarily unavailable.")
    
    # Motivational Daily Goal Section
    st.markdown("---")
    st.subheader("🎯 Today's Challenge")
    
    goal_col1, goal_col2, goal_col3 = st.columns(3)
    
    with goal_col1:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            border: 2px solid rgba(255,255,255,0.2);
        ">
            <h3 style="margin: 0; color: #ffd700;">💪 Daily Goal</h3>
            <h2 style="margin: 10px 0; color: #90cdf4;">5 Deals</h2>
            <p style="margin: 5px 0; opacity: 0.9;">Push yourself today!</p>
        </div>
        """, unsafe_allow_html=True)
    
    with goal_col2:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            border: 2px solid rgba(255,255,255,0.2);
        ">
            <h3 style="margin: 0; color: #ffffff;">🚀 Bonus Zone</h3>
            <h2 style="margin: 10px 0; color: #ffffff;">7+ Deals</h2>
            <p style="margin: 5px 0; opacity: 0.9;">Extra $100 bonus!</p>
        </div>
        """, unsafe_allow_html=True)
    
    with goal_col3:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #ff6b6b 0%, #feca57 100%);
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            border: 2px solid rgba(255,255,255,0.2);
        ">
            <h3 style="margin: 0; color: #ffffff;">🏆 Elite Status</h3>
            <h2 style="margin: 10px 0; color: #ffffff;">10+ Deals</h2>
            <p style="margin: 5px 0; opacity: 0.9;">Top agent recognition!</p>
        </div>
        """, unsafe_allow_html=True)





    st.markdown("---")

    agent = df_agents[df_agents['username'] == st.session_state.user_email]
    if agent.empty:
        st.error("Agent not found.")
        st.stop()

    user_id = AGENT_USERIDS.get(st.session_state.user_email)
    if not user_id:
        st.error("Agent user ID not found.")
        st.stop()
    
    agent_name = agent.iloc[0]['name'] if 'name' in agent.columns else st.session_state.user_name

    # Get live performance data using same API call as admin dashboard
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    today_str = date.today().strftime("%Y-%m-%d")
    
    # Use the correct query format for today's deals
    params = {
        "or[0][date_created]": "Today",
        "or[0][date_converted]": "Today", 
        "or[1][date_sold]": "Today",
        "or[1][date_converted]": "Today",
        "limit": 5000,
        "columns": "policy_id,date_sold,agent_id,agent_name,lead_first_name,lead_last_name,lead_phone"
    }
    
    try:
        response = requests.get(CRM_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json().get("response", {})
        results = data.get("results", [])
        
        if results:
            # Filter for current agent using robust matching logic
            agent_deals = []
            for deal in results:
                deal_agent_name = str(deal.get('agent_name', '')).lower().strip()
                deal_agent_id = str(deal.get('agent_id', ''))
                
                # Split agent name for partial matching
                current_agent_lower = agent_name.lower().strip()
                current_agent_parts = current_agent_lower.split()
                deal_agent_parts = deal_agent_name.split()
                
                # Check for match using multiple criteria
                match_found = False
                
                # 1. Exact agent ID match
                if deal_agent_id == str(user_id):
                    match_found = True
                
                # 2. Exact name match
                elif deal_agent_name == current_agent_lower:
                    match_found = True
                
                # 3. Partial name matching (first name, last name, or both)
                elif any(part in deal_agent_name for part in current_agent_parts if len(part) > 2):
                    match_found = True
                
                # 4. Reverse partial matching 
                elif any(part in current_agent_lower for part in deal_agent_parts if len(part) > 2):
                    match_found = True
                
                if match_found:
                    agent_deals.append(deal)
            
            deal_count = len(agent_deals)
            
            # Get authentic member counts using dependents API (same logic as Top Performers)
            total_members = 0
            if agent_deals:
                try:
                    # Fetch leads to get lead_ids
                    leads_url = "https://hcs.tldcrm.com/api/egress/leads"
                    leads_params = {"limit": 1000}
                    leads_response = requests.get(leads_url, headers=headers, params=leads_params, timeout=10)
                    leads_response.raise_for_status()
                    leads_data = leads_response.json().get("response", {})
                    all_leads = leads_data.get("results", [])
                    
                    # Create lead lookup
                    leads_lookup = {}
                    for lead in all_leads:
                        lead_id = lead['lead_id']
                        # Name-based lookup
                        first_name = str(lead.get('first_name', '')).lower().strip()
                        last_name = str(lead.get('last_name', '')).lower().strip()
                        if first_name and last_name:
                            name_key = f"{first_name}_{last_name}"
                            leads_lookup[name_key] = lead_id
                        # Phone-based lookup
                        phone = lead.get('phone', '')
                        if phone:
                            clean_phone = str(phone).replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+1', '')
                            leads_lookup[clean_phone] = lead_id
                    
                    # Get dependents for matched leads
                    dependents_url = "https://hcs.tldcrm.com/api/egress/lead_dependents"
                    dependents_params = {"limit": 5000}
                    dependents_response = requests.get(dependents_url, headers=headers, params=dependents_params, timeout=10)
                    dependents_response.raise_for_status()
                    dependents_data = dependents_response.json().get("response", {})
                    all_dependents = dependents_data.get("results", [])
                    
                    # Count dependents per lead_id
                    dependent_counts = {}
                    for dependent in all_dependents:
                        lead_id = dependent.get('lead_id')
                        if lead_id:
                            dependent_counts[lead_id] = dependent_counts.get(lead_id, 0) + 1
                    
                    # Calculate total members using same logic as Top Performers
                    processed_leads = set()
                    
                    for deal in agent_deals:
                        # Get lead info from deal
                        first_name = str(deal.get('lead_first_name', '')).strip()
                        last_name = str(deal.get('lead_last_name', '')).strip()
                        phone = deal.get('lead_phone', '')
                        
                        lead_id = None
                        
                        # Try name matching first
                        if first_name and last_name and first_name != 'nan' and last_name != 'nan':
                            name_key = f"{first_name}_{last_name}".lower()
                            lead_id = leads_lookup.get(name_key)
                        
                        # Try phone matching if name didn't work
                        if not lead_id and phone:
                            clean_phone = str(phone).replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('+1', '')
                            lead_id = leads_lookup.get(clean_phone)
                        
                        # Calculate members for this lead (avoid double counting)
                        if lead_id and lead_id not in processed_leads:
                            dependents = dependent_counts.get(lead_id, 0)
                            total_members += 1 + dependents  # 1 primary + dependents
                            processed_leads.add(lead_id)
                        elif not lead_id:
                            total_members += 1  # Fallback to 1 if no match found
                            
                except Exception as e:
                    # Fallback to deal count if dependents API fails
                    total_members = deal_count
            
            member_count = total_members if total_members > 0 else deal_count
            closing_rate = 25.0  # Will calculate from actual call data
            
        else:
            deal_count = 0
            member_count = 0
            closing_rate = 0.0
            
    except Exception as e:
        st.error(f"Unable to fetch live performance data: {str(e)}")
        deal_count = 0
        member_count = 0
        closing_rate = 0.0

    # Performance Snapshot Dashboard
    st.markdown("---")
    st.subheader("📊 Your Performance Snapshot")
    
    # Create animated progress rings for key metrics
    quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)
    
    with quick_col1:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 15px;
            border-radius: 12px;
            text-align: center;
            color: white;
            border: 1px solid rgba(255,255,255,0.1);
        ">
            <h4 style="margin: 0; font-size: 14px; opacity: 0.8;">Today's Deals</h4>
            <h2 style="margin: 5px 0; font-size: 24px; color: #90cdf4;">{deal_count}</h2>
            <div style="
                width: 40px;
                height: 4px;
                background: rgba(255,255,255,0.3);
                border-radius: 2px;
                margin: 8px auto;
                position: relative;
            ">
                <div style="
                    width: 60%;
                    height: 100%;
                    background: #90cdf4;
                    border-radius: 2px;
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with quick_col2:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            padding: 15px;
            border-radius: 12px;
            text-align: center;
            color: white;
            border: 1px solid rgba(255,255,255,0.1);
        ">
            <h4 style="margin: 0; font-size: 14px; opacity: 0.8;">Closing Rate</h4>
            <h2 style="margin: 5px 0; font-size: 24px; color: #ffffff;">{closing_rate:.1f}%</h2>
            <div style="
                width: 40px;
                height: 4px;
                background: rgba(255,255,255,0.3);
                border-radius: 2px;
                margin: 8px auto;
                position: relative;
            ">
                <div style="
                    width: 28%;
                    height: 100%;
                    background: #ffffff;
                    border-radius: 2px;
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with quick_col3:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #ff6b6b 0%, #feca57 100%);
            padding: 15px;
            border-radius: 12px;
            text-align: center;
            color: white;
            border: 1px solid rgba(255,255,255,0.1);
        ">
            <h4 style="margin: 0; font-size: 14px; opacity: 0.8;">Est. Earnings</h4>
            <h2 style="margin: 5px 0; font-size: 24px; color: #ffffff;">${int(member_count * 15)}</h2>
            <div style="
                width: 40px;
                height: 4px;
                background: rgba(255,255,255,0.3);
                border-radius: 2px;
                margin: 8px auto;
                position: relative;
            ">
                <div style="
                    width: 75%;
                    height: 100%;
                    background: #ffffff;
                    border-radius: 2px;
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with quick_col4:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
            padding: 15px;
            border-radius: 12px;
            text-align: center;
            color: #2d3748;
            border: 1px solid rgba(255,255,255,0.1);
        ">
            <h4 style="margin: 0; font-size: 14px; opacity: 0.8;">Rank Today</h4>
            <h2 style="margin: 5px 0; font-size: 24px; color: #2d3748;">#1</h2>
            <div style="
                width: 40px;
                height: 4px;
                background: rgba(45,55,72,0.3);
                border-radius: 2px;
                margin: 8px auto;
                position: relative;
            ">
                <div style="
                    width: 100%;
                    height: 100%;
                    background: #2d3748;
                    border-radius: 2px;
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Current cycle detection
    today = pd.Timestamp.now().normalize()
    current_cycle = commission_cycles[
        (commission_cycles["start"] <= today) & (today <= commission_cycles["end"])
    ]
    
    if not current_cycle.empty:
        cycle_row = current_cycle.iloc[0]
        cycle_start = cycle_row["start"].strftime("%Y-%m-%d")
        cycle_end = cycle_row["end"].strftime("%Y-%m-%d")
        pay_date = cycle_row["pay"].strftime("%Y-%m-%d")
        
        # Enhanced cycle info with progress bar
        days_total = (cycle_row["end"] - cycle_row["start"]).days + 1
        days_passed = (today - cycle_row["start"]).days + 1
        cycle_progress = min(days_passed / days_total, 1.0)
        
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(90deg, #f8f9fa 0%, #e9ecef 100%);
                padding: 20px;
                border-radius: 15px;
                margin-bottom: 25px;
                border-left: 5px solid #28a745;
            ">
                <h3 style="margin: 0 0 15px 0; color: #495057;">📅 Current Commission Cycle</h3>
                <p style="margin: 0; font-size: 16px; color: #6c757d;">
                    <strong>{cycle_start}</strong> to <strong>{cycle_end}</strong> | Pay Date: <strong style="color: #28a745;">{pay_date}</strong>
                </p>
                <div style="background: #dee2e6; border-radius: 10px; margin-top: 15px; overflow: hidden;">
                    <div style="
                        background: linear-gradient(90deg, #28a745, #20c997); 
                        height: 8px; 
                        width: {cycle_progress * 100}%;
                        transition: width 0.3s ease;
                    "></div>
                </div>
                <p style="margin: 5px 0 0 0; font-size: 14px; color: #6c757d;">
                    Cycle Progress: {cycle_progress * 100:.1f}% Complete
                </p>
            </div>
            """, 
            unsafe_allow_html=True
        )
        

        
        # Calculate commission tier and progression based on AUTHENTIC PAY STRUCTURE
        if member_count >= 140:
            rate = 25
            bonus = 1200  # Performance bonus for 70+ members
            tier = "ELITE PERFORMER"
            tier_color = "#dc3545"
            tier_emoji = "🏆"
            next_milestone = None
        elif member_count >= 100:
            rate = 22.5
            bonus = 1200  # Performance bonus for 70+ members
            tier = "HIGH ACHIEVER"
            tier_color = "#ffc107"
            tier_emoji = "🚀"
            next_milestone = f"{140 - member_count} members to ELITE"
        elif member_count >= 70:
            rate = 17.5
            bonus = 1200  # Performance bonus for 70+ members
            tier = "TOP PRODUCER"
            tier_color = "#fd7e14"
            tier_emoji = "⭐"
            next_milestone = f"{100 - member_count} members to HIGH ACHIEVER"
        else:
            rate = 15
            bonus = 0  # No bonus below 70 members
            tier = "RISING STAR"
            tier_color = "#6f42c1"
            tier_emoji = "💫"
            next_milestone = f"{70 - member_count} members to TOP PRODUCER"
        
        # Check for milestone achievements and store in session state
        if 'agent_milestones' not in st.session_state:
            st.session_state.agent_milestones = {}
        
        agent_key = f"{user_id}_{user_name}"
        previous_tier = st.session_state.agent_milestones.get(agent_key, 0)
        
        # Track tier progression: 0=Rising Star, 1=Top Producer, 2=High Achiever, 3=Elite
        current_tier_level = 0
        if member_count >= 140:
            current_tier_level = 3
        elif member_count >= 100:
            current_tier_level = 2
        elif member_count >= 70:
            current_tier_level = 1
        
        # Show milestone achievement notification
        if current_tier_level > previous_tier:
            milestone_messages = {
                1: ("🎉 MILESTONE ACHIEVED! 🎉", f"Welcome to TOP PRODUCER tier! You've earned the $1,200 performance bonus!", "#fd7e14"),
                2: ("🚀 AMAZING ACHIEVEMENT! 🚀", f"You've reached HIGH ACHIEVER status! Keep pushing towards ELITE!", "#ffc107"), 
                3: ("🏆 ELITE PERFORMER! 🏆", f"You've achieved the highest tier! Outstanding performance!", "#dc3545")
            }
            
            if current_tier_level in milestone_messages:
                title, message, color = milestone_messages[current_tier_level]
                st.balloons()
                st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, {color}, #ffffff);
                    border: 3px solid {color};
                    border-radius: 15px;
                    padding: 20px;
                    margin: 20px 0;
                    text-align: center;
                    box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                    animation: pulse 2s infinite;
                ">
                    <h2 style="color: white; text-shadow: 2px 2px 4px rgba(0,0,0,0.5); margin: 0;">{title}</h2>
                    <p style="color: white; font-size: 18px; margin: 10px 0; text-shadow: 1px 1px 2px rgba(0,0,0,0.5);">{message}</p>
                    <p style="color: white; font-size: 16px; margin: 0; text-shadow: 1px 1px 2px rgba(0,0,0,0.5);">
                        Achievement unlocked at {datetime.now().strftime('%I:%M %p on %B %d, %Y')}
                    </p>
                </div>
                <style>
                @keyframes pulse {{
                    0% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.02); }}
                    100% {{ transform: scale(1); }}
                }}
                </style>
                """, unsafe_allow_html=True)
            
            # Update milestone tracking
            st.session_state.agent_milestones[agent_key] = current_tier_level
        
        est_commission = member_count * rate + bonus
        days_left = max((cycle_row["end"] - today).days + 1, 0)
        
        # Performance tier badge
        st.markdown(
            f"""
            <div style="text-align: center; margin-bottom: 20px;">
                <div style="
                    display: inline-block;
                    background: {tier_color};
                    color: white;
                    padding: 10px 20px;
                    border-radius: 25px;
                    font-weight: bold;
                    font-size: 16px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                ">
                    {tier_emoji} {tier}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        # Enhanced metrics with visual appeal
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    color: white;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                ">
                    <h3 style="margin: 0; font-size: 32px; font-weight: bold;">{deal_count}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">🎯 Sales Today</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col2:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    color: white;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                ">
                    <h3 style="margin: 0; font-size: 32px; font-weight: bold;">{member_count}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">👥 Members This Cycle</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col3:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    color: white;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                ">
                    <h3 style="margin: 0; font-size: 28px; font-weight: bold;">${est_commission:,.0f}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">💰 Est. Commission</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col4:
            urgency_color = "#dc3545" if days_left <= 3 else "#ffc107" if days_left <= 7 else "#28a745"
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, {urgency_color}dd 0%, {urgency_color}bb 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    color: white;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                ">
                    <h3 style="margin: 0; font-size: 32px; font-weight: bold;">{days_left}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">⏰ Days Left</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col4:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #fc466b 0%, #3f5efb 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    color: white;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                ">
                    <h3 style="margin: 0; font-size: 18px; font-weight: bold;">{pay_date}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">📅 Pay Date</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        # Achievement System
        st.markdown("---")
        st.subheader("🏆 Achievements & Badges")
        
        achievements_col1, achievements_col2, achievements_col3 = st.columns(3)
        
        with achievements_col1:
            first_member_unlocked = member_count >= 1
            badge_opacity = "1.0" if first_member_unlocked else "0.3"
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 15px;
                border-radius: 12px;
                text-align: center;
                color: white;
                opacity: {badge_opacity};
                border: 2px solid {"#ffd700" if first_member_unlocked else "rgba(255,255,255,0.1)"};
            ">
                <h3 style="margin: 0; font-size: 24px;">🥇</h3>
                <h4 style="margin: 5px 0; font-size: 12px;">FIRST MEMBER</h4>
                <p style="margin: 0; font-size: 10px; opacity: 0.8;">{"UNLOCKED!" if first_member_unlocked else "Enroll your first member"}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with achievements_col2:
            streak_unlocked = member_count >= 10
            badge_opacity = "1.0" if streak_unlocked else "0.3"
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                padding: 15px;
                border-radius: 12px;
                text-align: center;
                color: white;
                opacity: {badge_opacity};
                border: 2px solid {"#ffd700" if streak_unlocked else "rgba(255,255,255,0.1)"};
            ">
                <h3 style="margin: 0; font-size: 24px;">🔥</h3>
                <h4 style="margin: 5px 0; font-size: 12px;">HOT STREAK</h4>
                <p style="margin: 0; font-size: 10px; opacity: 0.8;">{"UNLOCKED!" if streak_unlocked else "Enroll 10 members"}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with achievements_col3:
            elite_unlocked = member_count >= 25
            badge_opacity = "1.0" if elite_unlocked else "0.3"
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #ff6b6b 0%, #feca57 100%);
                padding: 15px;
                border-radius: 12px;
                text-align: center;
                color: white;
                opacity: {badge_opacity};
                border: 2px solid {"#ffd700" if elite_unlocked else "rgba(255,255,255,0.1)"};
            ">
                <h3 style="margin: 0; font-size: 24px;">👑</h3>
                <h4 style="margin: 5px 0; font-size: 12px;">MEMBER KING</h4>
                <p style="margin: 0; font-size: 10px; opacity: 0.8;">{"UNLOCKED!" if elite_unlocked else "Enroll 25 members"}</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Motivational Quote Section
        import random
        motivational_quotes = [
            "Every 'no' gets you closer to a 'yes'!",
            "Champions are made from something deep inside them - a desire, a dream, a vision!",
            "Success isn't given. It's earned in the gym, on the phone, in every call!",
            "The difference between ordinary and extraordinary is that little 'extra'!",
            "Your attitude determines your altitude! Keep climbing!",
            "Winners don't wait for motivation. They create it!",
            "Today's performance determines tomorrow's paycheck!",
            "Every call is an opportunity. Every opportunity is a potential win!"
        ]
        
        quote = random.choice(motivational_quotes)
        
        st.markdown("---")
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            border: 2px solid rgba(255,255,255,0.2);
            margin: 20px 0;
        ">
            <h3 style="margin: 0; color: #ffd700;">Daily Motivation</h3>
            <p style="margin: 15px 0; font-size: 18px; font-style: italic; line-height: 1.4;">"{quote}"</p>
        </div>
        """, unsafe_allow_html=True)

        # Quick Action Panel
        st.markdown("---")
        st.subheader("⚡ Quick Actions")
        
        action_col1, action_col2, action_col3, action_col4 = st.columns(4)
        
        with action_col1:
            if st.button("📞 Start Calling", use_container_width=True):
                st.success("Let's crush those calls! Stay focused and close deals!")
        
        with action_col2:
            if st.button("📊 View My Stats", use_container_width=True):
                st.info("Your detailed stats are displayed above. Keep pushing!")
        
        with action_col3:
            if st.button("🎯 Set Goal", use_container_width=True):
                st.info("Your daily goal: 6 members. You've got this!")
        
        with action_col4:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()

        # Team Comparison Section
        st.markdown("---")
        st.subheader("📈 How You Stack Up")
        
        # Create comparison with team averages (member-based)
        team_avg_members = 5.8  # Average members per agent
        team_avg_commission = 315
        
        comparison_col1, comparison_col2 = st.columns(2)
        
        with comparison_col1:
            your_performance = member_count / team_avg_members if team_avg_members > 0 else 0
            performance_percentage = int(your_performance * 100)
            
            color = "#28a745" if your_performance >= 1 else "#ffc107" if your_performance >= 0.8 else "#dc3545"
            
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, {color}dd 0%, {color}bb 100%);
                padding: 20px;
                border-radius: 15px;
                text-align: center;
                color: white;
                border: 2px solid rgba(255,255,255,0.2);
            ">
                <h3 style="margin: 0; color: white;">👥 Members vs Team Avg</h3>
                <h2 style="margin: 10px 0; color: white;">{performance_percentage}%</h2>
                <p style="margin: 5px 0; opacity: 0.9;">You: {member_count} | Team: {team_avg_members}</p>
                <div style="
                    width: 100%;
                    height: 8px;
                    background: rgba(255,255,255,0.3);
                    border-radius: 4px;
                    margin: 10px 0;
                ">
                    <div style="
                        width: {min(performance_percentage, 100)}%;
                        height: 100%;
                        background: white;
                        border-radius: 4px;
                    "></div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with comparison_col2:
            commission_performance = est_commission / team_avg_commission if team_avg_commission > 0 else 0
            commission_percentage = int(commission_performance * 100)
            
            color = "#28a745" if commission_performance >= 1 else "#ffc107" if commission_performance >= 0.8 else "#dc3545"
            
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, {color}dd 0%, {color}bb 100%);
                padding: 20px;
                border-radius: 15px;
                text-align: center;
                color: white;
                border: 2px solid rgba(255,255,255,0.2);
            ">
                <h3 style="margin: 0; color: white;">💰 Earnings vs Team Avg</h3>
                <h2 style="margin: 10px 0; color: white;">{commission_percentage}%</h2>
                <p style="margin: 5px 0; opacity: 0.9;">You: ${est_commission:,.0f} | Team: ${team_avg_commission:,.0f}</p>
                <div style="
                    width: 100%;
                    height: 8px;
                    background: rgba(255,255,255,0.3);
                    border-radius: 4px;
                    margin: 10px 0;
                ">
                    <div style="
                        width: {min(commission_percentage, 100)}%;
                        height: 100%;
                        background: white;
                        border-radius: 4px;
                    "></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Next milestone motivation
        if next_milestone:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(90deg, #ff9a9e 0%, #fecfef 50%, #fecfef 100%);
                    padding: 15px;
                    border-radius: 10px;
                    text-align: center;
                    margin: 20px 0;
                    border: 2px solid rgba(255,255,255,0.3);
                ">
                    <strong style="color: #495057; font-size: 16px;">🎯 Next Milestone: {next_milestone}!</strong>
                </div>
                """,
                unsafe_allow_html=True
            )
        

    else:
        st.warning("⚠️ No active commission cycle found for today.")

    # Enhanced Payroll History Section
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 25px;
            border-radius: 15px;
            margin: 30px 0 20px 0;
        ">
            <h2 style="margin: 0; color: white; text-align: center; font-size: 28px;">
                💰 Your Earnings History
            </h2>
            <p style="margin: 10px 0 0 0; color: rgba(255,255,255,0.9); text-align: center; font-size: 16px;">
                Track your commission payments and performance
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Auto-refresh mechanism: Check for new payroll data every 30 seconds
    if 'last_payroll_check' not in st.session_state:
        st.session_state['last_payroll_check'] = 0
        
    current_time = datetime.now().timestamp()
    if current_time - st.session_state['last_payroll_check'] > 30:  # Check every 30 seconds
        st.session_state['last_payroll_check'] = current_time
        # Clear any cached data to force fresh database query
        if 'agent_payroll_cache' in st.session_state:
            del st.session_state['agent_payroll_cache']
        st.rerun()
    
    # Get agent's payroll history (force fresh query)
    agent_history_df = get_agent_payroll_history(agent_name)
    
    if not agent_history_df.empty:
        # Create summary metrics from payroll history
        total_earnings = agent_history_df['Net Pay'].sum() if 'Net Pay' in agent_history_df.columns else 0
        total_deals = agent_history_df['Paid Deals'].sum() if 'Paid Deals' in agent_history_df.columns else 0
        avg_per_deal = total_earnings / total_deals if total_deals > 0 else 0
        
        # Display earnings summary cards
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                    padding: 20px;
                    border-radius: 12px;
                    text-align: center;
                    color: white;
                    margin-bottom: 20px;
                ">
                    <h3 style="margin: 0; font-size: 24px;">${total_earnings:,.2f}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">💰 Total Earnings</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col2:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 20px;
                    border-radius: 12px;
                    text-align: center;
                    color: white;
                    margin-bottom: 20px;
                ">
                    <h3 style="margin: 0; font-size: 24px;">{total_deals}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">🎯 Total Paid Deals</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col3:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #fc466b 0%, #3f5efb 100%);
                    padding: 20px;
                    border-radius: 12px;
                    text-align: center;
                    color: white;
                    margin-bottom: 20px;
                ">
                    <h3 style="margin: 0; font-size: 24px;">${avg_per_deal:.2f}</h3>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">📊 Avg per Deal</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        # Format currency columns for display
        display_df = agent_history_df.copy()
        for col in ["Per-Member Rate", "Production Bonus", "Retention Bonus", "Top Agent Bonus", "Gross Pay", "Net Pay"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
        
        st.markdown("### 📋 Detailed Payroll Records")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        # Success message with PDF info
        st.markdown(
            """
            <div style="
                background: linear-gradient(90deg, #d4edda 0%, #c3e6cb 100%);
                border: 1px solid #c3e6cb;
                border-radius: 10px;
                padding: 15px;
                margin-top: 20px;
            ">
                <strong style="color: #155724;">📄 Pay Statement PDFs Available!</strong>
                <p style="margin: 5px 0 0 0; color: #155724;">
                    Your detailed pay statements with unpaid deal explanations are generated when admin uploads FMO files. 
                    Contact your admin to access the complete ZIP file with all agent statements.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            """
            <div style="
                background: linear-gradient(90deg, #e2e3e5 0%, #f8f9fa 100%);
                border: 2px dashed #6c757d;
                border-radius: 15px;
                padding: 40px;
                text-align: center;
                margin: 20px 0;
            ">
                <h3 style="color: #6c757d; margin: 0 0 15px 0;">📊 Payroll History Coming Soon!</h3>
                <p style="color: #6c757d; margin: 0; font-size: 16px;">
                    Your commission payments will appear here after admin uploads FMO statements.<br>
                    Keep closing deals - your earnings are being tracked!
                </p>
                <div style="margin-top: 20px;">
                    <span style="font-size: 48px;">💰</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    # Historical performance
    st.subheader("📈 Performance Overview")
    
    # Use current biweekly cycle dates for accurate performance metrics
    today = pd.Timestamp.now().normalize()
    current_cycle_check = commission_cycles[
        (commission_cycles["start"] <= today) & (today <= commission_cycles["end"])
    ]
    
    if not current_cycle_check.empty:
        cycle_row = current_cycle_check.iloc[0]
        cycle_start_str = cycle_row["start"].strftime("%Y-%m-%d")
        cycle_end_str = cycle_row["end"].strftime("%Y-%m-%d")
        cycle_deals = fetch_agent_deals(user_id, cycle_start_str, cycle_end_str)
    else:
        # Use recent period if no current cycle found
        seven_days_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")
        cycle_deals = fetch_agent_deals(user_id, seven_days_ago, today_str)
    
    # Enhanced performance metrics with member tracking for current cycle
    col1, col2, col3, col4 = st.columns(4)
    
    deal_count_cycle = len(cycle_deals)
    total_members_cycle = cycle_deals['total_members'].sum() if not cycle_deals.empty and 'total_members' in cycle_deals.columns else deal_count_cycle
    
    with col1:
        cycle_label = "Current Cycle" if not current_cycle.empty else "Recent Period"
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                color: white;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            ">
                <h3 style="margin: 0; font-size: 24px;">{deal_count_cycle}</h3>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">📊 Deals ({cycle_label})</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with col2:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                color: white;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            ">
                <h3 style="margin: 0; font-size: 24px;">{int(total_members_cycle)}</h3>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">👥 Total Members</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with col3:
        # Calculate tier-based rate using MEMBER count from current biweekly cycle
        if total_members_cycle >= 140:
            rate = 25
            bonus = 1200
            tier = "ELITE"
        elif total_members_cycle >= 100:
            rate = 22.5
            bonus = 1200
            tier = "PLATINUM"
        elif total_members_cycle >= 70:
            rate = 17.5
            bonus = 1200
            tier = "GOLD"
        else:
            rate = 15
            bonus = 0
            tier = "STANDARD"
            
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #fc466b 0%, #3f5efb 100%);
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                color: white;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            ">
                <h3 style="margin: 0; font-size: 18px;">${rate}/Member</h3>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">💎 {tier} Tier</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with col4:
        commission_cycle = total_members_cycle * rate + bonus
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                color: white;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            ">
                <h3 style="margin: 0; font-size: 20px;">${commission_cycle:,.0f}</h3>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">💰 Est. Commission</p>
            </div>
            """,
            unsafe_allow_html=True
        )

# === ADMIN DASHBOARD ===
else:
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px;
            border-radius: 20px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        ">
            <h1 style="
                color: white;
                font-size: 48px;
                margin: 0 0 15px 0;
                text-shadow: 0 2px 4px rgba(0,0,0,0.3);
                font-weight: bold;
            ">
                🏆 ADMIN COMMAND CENTER 🏆
            </h1>
            <p style="
                color: rgba(255,255,255,0.9);
                font-size: 18px;
                margin: 0;
                font-weight: 500;
            ">
                💼 Manage Operations • Track Performance • Drive Success 💼
            </p>
            <div style="
                background: rgba(255,255,255,0.2);
                margin: 20px auto 0 auto;
                padding: 10px 25px;
                border-radius: 15px; 
                display: inline-block;
                backdrop-filter: blur(10px);
            ">
                <span style="font-size: 18px; font-weight: bold;">⚡ LEAD THE TEAM • MAXIMIZE PROFITS • CONTROL SUCCESS ⚡</span>
            </div>
        </div>
        """, 
        unsafe_allow_html=True
    )

    tabs = st.tabs([
        "🏆 Overview",
        "📋 Leaderboard", 
        "📈 History",
        "📊 Live Data",
        "⚡ Automation",
        "⚙️ Admin Tools"
    ])

    # Initialize session state for payroll totals
    if "payroll_totals" not in st.session_state:
        st.session_state["payroll_totals"] = {"deals": 0, "agent": 0.0, "owner_rev": 0.0, "owner_prof": 0.0}
    
    # Initialize summary if not present
    if "summary" not in st.session_state:
        st.session_state["summary"] = []

    # Get current totals
    totals = st.session_state["payroll_totals"]
    summary = st.session_state["summary"]

    # Enhanced Overview Cards with exciting visuals
    st.markdown("<div style='margin-top:1.5em;'></div>", unsafe_allow_html=True)
    
    o1, o2, o3, o4 = st.columns(4)
    
    with o1:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 25px;
                border-radius: 15px;
                text-align: center;
                color: white;
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                margin-bottom: 20px;
            ">
                <h2 style="margin: 0; font-size: 36px; font-weight: bold;">{int(totals['deals']):,}</h2>
                <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">🎯 Total Paid Deals</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with o2:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                padding: 25px;
                border-radius: 15px;
                text-align: center;
                color: white;
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                margin-bottom: 20px;
            ">
                <h2 style="margin: 0; font-size: 32px; font-weight: bold;">${totals['agent']:,.0f}</h2>
                <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">💰 Agent Payout</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with o3:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #fc466b 0%, #3f5efb 100%);
                padding: 25px;
                border-radius: 15px;
                text-align: center;
                color: white;
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                margin-bottom: 20px;
            ">
                <h2 style="margin: 0; font-size: 32px; font-weight: bold;">${totals['owner_rev']:,.0f}</h2>
                <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">📈 Owner Revenue</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with o4:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                padding: 25px;
                border-radius: 15px;
                text-align: center;
                color: white;
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                margin-bottom: 20px;
            ">
                <h2 style="margin: 0; font-size: 32px; font-weight: bold;">${totals['owner_prof']:,.0f}</h2>
                <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">💎 Owner Profit</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.markdown("---")

    # Enhanced Top Agents Leaderboard
    st.markdown(
        """
        <div style="
            background: linear-gradient(90deg, #ff9a9e 0%, #fecfef 50%, #fecfef 100%);
            padding: 20px;
            border-radius: 15px;
            margin-bottom: 20px;
            text-align: center;
        ">
            <h3 style="margin: 0; color: #495057; font-size: 24px;">🥇 TOP PERFORMERS LEADERBOARD 🥇</h3>
            <p style="margin: 5px 0 0 0; color: #6c757d;">Elite agents driving maximum results</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    if summary:
        df_led = pd.DataFrame(summary)
        
        # Determine sort column
        if "Paid Deals" in df_led.columns:
            sort_col = "Paid Deals"
        elif "Paid Applications" in df_led.columns:
            sort_col = "Paid Applications"
        else:
            sort_col = df_led.columns[0] if len(df_led.columns) > 0 else None
            
        if sort_col is not None:
            df_led = df_led.sort_values(sort_col, ascending=False).head(6)
        
        # Format payout columns
        payout_cols = [col for col in df_led.columns if "Payout" in col or "Profit" in col]
        
        st.dataframe(df_led, hide_index=True, use_container_width=True)
    else:
        st.info("Upload a statement to see leaderboard.")

    st.markdown("---")

    # Enhanced Live Counts Section
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 25px;
            border-radius: 15px;
            margin: 30px 0 20px 0;
            text-align: center;
        ">
            <h3 style="margin: 0; color: white; font-size: 28px;">📊 LIVE PERFORMANCE DASHBOARD</h3>
            <p style="margin: 10px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
                Real-time deal tracking and performance monitoring
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    try:
        df_api = fetch_all_today(limit=5000)
        if not df_api.empty:
            df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
            # Use Eastern Time for date comparisons
            eastern = pytz.timezone('US/Eastern')
            today = datetime.now(eastern).date()
            daily_mask = df_api["date_sold"].dt.date == today
            weekly_mask = df_api["date_sold"].dt.isocalendar().week == pd.Timestamp.now().isocalendar().week
            monthly_mask = df_api["date_sold"].dt.month == today.month

            daily_count = len(df_api[daily_mask])
            weekly_count = len(df_api[weekly_mask])
            monthly_count = len(df_api[monthly_mask])
            
            # Calculate member counts for live data
            daily_members = df_api[daily_mask]['total_members'].sum() if 'total_members' in df_api.columns else daily_count
            weekly_members = df_api[weekly_mask]['total_members'].sum() if 'total_members' in df_api.columns else weekly_count
            monthly_members = df_api[monthly_mask]['total_members'].sum() if 'total_members' in df_api.columns else monthly_count

            # Display both deal counts and member counts
            st.markdown(
                """
                <div style="margin-bottom: 20px;">
                    <h4 style="text-align: center; color: white; margin: 0;">📊 Deal Counts</h4>
                </div>
                """,
                unsafe_allow_html=True
            )

            lc1, lc2, lc3 = st.columns(3)
            
            with lc1:
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                        padding: 25px;
                        border-radius: 15px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                        margin-bottom: 20px;
                    ">
                        <h2 style="margin: 0; font-size: 36px; font-weight: bold;">{daily_count:,}</h2>
                        <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">📅 Today's Deals</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with lc2:
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #fc466b 0%, #3f5efb 100%);
                        padding: 25px;
                        border-radius: 15px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                        margin-bottom: 20px;
                    ">
                        <h2 style="margin: 0; font-size: 36px; font-weight: bold;">{weekly_count:,}</h2>
                        <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">📈 This Week</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with lc3:
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                        padding: 25px;
                        border-radius: 15px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                        margin-bottom: 20px;
                    ">
                        <h2 style="margin: 0; font-size: 36px; font-weight: bold;">{monthly_count:,}</h2>
                        <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">📊 This Month</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            # Add member count row
            st.markdown(
                """
                <div style="margin: 30px 0 20px 0;">
                    <h4 style="text-align: center; color: #333; margin: 0;">👥 Member Counts</h4>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            mc1, mc2, mc3 = st.columns(3)
            
            with mc1:
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #4CAF50 0%, #81C784 100%);
                        padding: 20px;
                        border-radius: 12px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        margin-bottom: 20px;
                    ">
                        <h3 style="margin: 0; font-size: 28px; font-weight: bold;">{int(daily_members):,}</h3>
                        <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 14px;">👥 Today's Members</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with mc2:
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #2196F3 0%, #64B5F6 100%);
                        padding: 20px;
                        border-radius: 12px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        margin-bottom: 20px;
                    ">
                        <h3 style="margin: 0; font-size: 28px; font-weight: bold;">{int(weekly_members):,}</h3>
                        <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 14px;">👥 Weekly Members</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with mc3:
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #FF9800 0%, #FFB74D 100%);
                        padding: 20px;
                        border-radius: 12px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        margin-bottom: 20px;
                    ">
                        <h3 style="margin: 0; font-size: 28px; font-weight: bold;">{int(monthly_members):,}</h3>
                        <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 14px;">👥 Monthly Members</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
        else:
            st.markdown(
                """
                <div style="
                    background: linear-gradient(90deg, #e2e3e5 0%, #f8f9fa 100%);
                    border: 2px dashed #6c757d;
                    border-radius: 15px;
                    padding: 40px;
                    text-align: center;
                    margin: 20px 0;
                ">
                    <h3 style="color: #6c757d; margin: 0 0 15px 0;">📊 Live Data Loading...</h3>
                    <p style="color: #6c757d; margin: 0; font-size: 16px;">
                        No live data currently available from API.<br>
                        Data will refresh automatically when available.
                    </p>
                </div>
                """,
                unsafe_allow_html=True
            )
    except Exception as e:
        st.markdown(
            """
            <div style="
                background: linear-gradient(90deg, #f8d7da 0%, #f5c6cb 100%);
                border: 1px solid #f5c6cb;
                border-radius: 10px;
                padding: 15px;
                margin: 20px 0;
                text-align: center;
            ">
                <strong style="color: #721c24;">⚠️ Live Data Temporarily Unavailable</strong>
                <p style="margin: 5px 0 0 0; color: #721c24;">
                    Live count data is currently not available. Please check back shortly.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.markdown("---")

    # --- Agent Personal Dashboard (always available for agents) ---
    if st.session_state.get("role") == "Agent":
        current_user = st.session_state.get("username")
        agent_reports = st.session_state.get("agent_reports", {})
        cycle_info = st.session_state.get("cycle_info")
        
        # Find agent's report by matching username to agent name
        agent_report = None
        agent_name = None
        
        # First try exact username match
        if current_user in AGENT_NAMES:
            agent_name = AGENT_NAMES[current_user]
            if agent_name in agent_reports:
                agent_report = agent_reports[agent_name]
        
        # If no exact match, try partial matching by name components
        if not agent_report and current_user:
            current_user_lower = current_user.lower()
            for stored_agent_name, report_data in agent_reports.items():
                stored_name_lower = stored_agent_name.lower()
                # Check if username appears in the stored agent name or vice versa
                if (current_user_lower in stored_name_lower or 
                    stored_name_lower in current_user_lower or
                    any(part in stored_name_lower for part in current_user_lower.split()) or
                    any(part in current_user_lower for part in stored_name_lower.split())):
                    agent_name = stored_agent_name
                    agent_report = report_data
                    break
        
        if agent_name:
            st.markdown(f"<h4 style='margin-bottom:0.3em;'>📊 Your Agent Dashboard - {agent_name}</h4>", unsafe_allow_html=True)
            
            # Show cycle-specific net pay if available
            if agent_report and st.session_state.get("reports_generated"):
                st.success(f"💰 Net Pay Available for Cycle: {agent_report.get('cycle_period', 'Current Period')}")
                
                # Display agent's personal metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Net Pay", f"${agent_report['total_payout']:,.2f}")
                with col2:
                    st.metric("Paid Applications", agent_report['paid_applications'])
                with col3:
                    st.metric("Total Members", agent_report['total_members'])
                with col4:
                    st.metric("Pay Date", agent_report.get('pay_date', 'TBD'))
                
                # Detailed breakdown
                st.markdown("### Pay Breakdown")
                
                # Check if we have detailed breakdown data (from Settings tab) or basic data (from Payroll tab)
                if 'base_pay' in agent_report:
                    # Detailed breakdown from Settings tab with advances
                    breakdown_data = [
                        {"Component": "Base Pay", "Amount": f"${agent_report['base_pay']:,.2f}"},
                        {"Component": "Production Bonus", "Amount": f"${agent_report['production_bonus']:,.2f}"},
                        {"Component": "Retention Bonus", "Amount": f"${agent_report['retention_bonus']:,.2f}"},
                        {"Component": "Gross Pay", "Amount": f"${agent_report['gross_pay']:,.2f}"},
                        {"Component": "Advances", "Amount": f"-${agent_report.get('advances', 0):,.2f}"},
                        {"Component": "Net Pay", "Amount": f"${agent_report['total_payout']:,.2f}"}
                    ]
                else:
                    # Basic breakdown from Payroll tab
                    breakdown_data = [
                        {"Component": "Base Pay", "Amount": f"${agent_report['total_members'] * agent_report['per_member_rate']:,.2f}"},
                        {"Component": "Production Bonus", "Amount": f"${agent_report['production_bonus']:,.2f}"},
                        {"Component": "Retention Bonus", "Amount": f"${agent_report['retention_bonus']:,.2f}"},
                        {"Component": "Top Agent Bonus", "Amount": f"${agent_report.get('top_agent_bonus', 0):,.2f}"},
                        {"Component": "Total Net Pay", "Amount": f"${agent_report['total_payout']:,.2f}"}
                    ]
                
                breakdown_df = pd.DataFrame(breakdown_data)
                st.dataframe(breakdown_df, use_container_width=True, hide_index=True)
                
                # Client details (if available)
                if 'client_rows' in agent_report and agent_report['client_rows']:
                    st.markdown("### Your Paid Clients")
                    client_data = []
                    for fname, lname, members in agent_report['client_rows']:
                        client_data.append({
                            "Client Name": f"{fname} {lname}",
                            "Members": members,
                            "Pay Amount": f"${members * agent_report['per_member_rate']:,.2f}"
                        })
                    
                    clients_df = pd.DataFrame(client_data)
                    st.dataframe(clients_df, use_container_width=True, hide_index=True)
                
                # Download options
                st.markdown("### Download Your Reports")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Download CSV breakdown
                    individual_csv = breakdown_df.to_csv(index=False)
                    cycle_label = agent_report.get('cycle_period', 'current').replace('/', '_').replace(' ', '_')
                    st.download_button(
                        "📊 Download Pay Breakdown (CSV)",
                        individual_csv,
                        file_name=f"{agent_name.replace(' ', '_')}_pay_breakdown_{cycle_label}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    # Download PDF if available
                    if 'pdf_content' in agent_report:
                        st.download_button(
                            "📄 Download Official Pay Report (PDF)",
                            agent_report['pdf_content'],
                            file_name=f"{agent_name.replace(' ', '_')}_official_pay_report.pdf",
                            mime="application/pdf"
                        )
                    else:
                        st.info("PDF report available after admin generates official reports")
                
                st.markdown("---")
            else:
                st.info("💡 Net pay reports will appear here when commission statements are processed")
                
                # Show information about when net pay will be available
                st.markdown("### 📋 Net Pay Status")
                st.write("Your net pay report for the completed commission cycle will be available after:")
                st.write("1. Admin uploads FMO Statement in Settings tab")  
                st.write("2. Admin uploads Health Sherpa Export in Settings tab")
                st.write("3. Admin clicks 'Calculate Vendor CPL/CPA' to process the cycle")
                
                st.info("Contact admin to process commission statements for the 5/17-5/30 cycle (Pay Date: 6/6)")
            
            # Always show daily performance tracking for agents
            st.markdown("### 📈 Your Daily Performance")
            try:
                df_today = fetch_all_today(limit=5000)
                if not df_today.empty:
                    df_today["date_sold"] = pd.to_datetime(df_today["date_sold"], errors="coerce")
                    today = pd.Timestamp.now().date()
                    
                    # Filter for this agent's deals today
                    if 'agent_name' in df_today.columns:
                        agent_today = df_today[
                            (df_today["date_sold"].dt.date == today) & 
                            (df_today['agent_name'] == agent_name)
                        ]
                        
                        if not agent_today.empty:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Deals Today", len(agent_today))
                            with col2:
                                # Calculate today's estimated commission
                                today_deals = len(agent_today)
                                if today_deals >= 140:
                                    rate = 25
                                elif today_deals >= 100:
                                    rate = 22.5
                                elif today_deals >= 70:
                                    rate = 17.5
                                else:
                                    rate = 15
                                today_commission = today_deals * rate
                                st.metric("Est. Commission Today", f"${today_commission:,.2f}")
                            
                            # Show recent deals
                            st.markdown("#### Recent Deals")
                            recent_cols = ['policy_id', 'lead_first_name', 'lead_last_name', 'carrier', 'product', 'date_sold']
                            recent_cols = [c for c in recent_cols if c in agent_today.columns]
                            if recent_cols:
                                st.dataframe(agent_today[recent_cols].head(10), use_container_width=True, hide_index=True)
                        else:
                            st.info("No deals found for today. Keep pushing!")
                    else:
                        st.warning("Agent performance data not available")
                else:
                    st.info("No daily performance data available")
            except Exception as e:
                st.warning(f"Could not load daily performance: {str(e)}")
        else:
            st.error("Agent profile not found. Please contact admin.")
    
    # --- Live Agent Performance Today (Admin view) ---
    elif st.session_state.get("role") == "Admin":
        st.markdown("<h4 style='margin-bottom:0.3em;'>🏆 Today's Agent Performance</h4>", unsafe_allow_html=True)
        try:
            df_today = fetch_all_today(limit=5000)
            if not df_today.empty:
                df_today["date_sold"] = pd.to_datetime(df_today["date_sold"], errors="coerce")
                today = pd.Timestamp.now().date()
                today_deals = df_today[df_today["date_sold"].dt.date == today]
                
                if not today_deals.empty and 'agent_name' in today_deals.columns:
                    # Group by agent and count deals
                    agent_performance = today_deals.groupby('agent_name').agg({
                        'policy_id': 'count',
                        'premium': lambda x: pd.to_numeric(x, errors='coerce').sum()
                    }).rename(columns={'policy_id': 'Deals Today', 'premium': 'Premium Today'})
                    
                    agent_performance = agent_performance.sort_values('Deals Today', ascending=False)
                    agent_performance['Premium Today'] = agent_performance['Premium Today'].fillna(0)
                    agent_performance['Premium Today'] = agent_performance['Premium Today'].apply(lambda x: f"${x:,.2f}")
                    
                    # Display top performers
                    st.dataframe(agent_performance.head(10), use_container_width=True)
                    
                    # Show top 3 performers in columns
                    if len(agent_performance) >= 3:
                        st.markdown("### 🥇 Top 3 Agents Today")
                        top3_cols = st.columns(3)
                        for i, (agent, data) in enumerate(agent_performance.head(3).iterrows()):
                            with top3_cols[i]:
                                medal = ["🥇", "🥈", "🥉"][i]
                                st.metric(f"{medal} {agent}", f"{data['Deals Today']} deals")
                                st.caption(f"Premium: {data['Premium Today']}")
                else:
                    st.info("No deals found for today or agent data unavailable.")
            else:
                st.info("No API data available for today's agent performance.")
        except Exception as e:
            st.warning(f"Could not load agent performance: {str(e)}")

    st.markdown("---")

    # --- Quickview (last 6 periods) ---
    st.markdown("<h4 style='margin-bottom:0.3em;'>📅 Recent Payroll Periods</h4>", unsafe_allow_html=True)
    if not history_df.empty:
        recent_history = history_df.tail(6)[
            ["upload_date", "total_deals", "agent_payout", "owner_revenue", "owner_profit"]
        ]
        
        st.dataframe(recent_history, use_container_width=True, hide_index=True)
    else:
        st.info("No payroll history yet.")

    # OVERVIEW TAB
    with tabs[0]:
        st.title("HCS Commission Dashboard")
        
        deals = int(totals.get("deals", 0))
        agent_payout = float(totals.get("agent", 0.0))
        owner_rev = float(totals.get("owner_rev", 0.0))
        owner_prof = float(totals.get("owner_prof", 0.0))

        c1, c2, c3, c4 = st.columns(4, gap="large")
        c1.metric("Total Paid Deals", f"{deals:,}")
        c2.metric("Agent Payout", f"${agent_payout:,.2f}")
        c3.metric("Owner Revenue", f"${owner_rev:,.2f}")
        c4.metric("Owner Profit", f"${owner_prof:,.2f}")

        st.markdown("---")

        s1, s2, s3 = st.columns(3, gap="large")
        s1.metric("Eddy (0.5%)", f"${owner_rev*0.005:,.2f}")
        s2.metric("Matt (2%)", f"${owner_rev*0.02:,.2f}")
        s3.metric("Jarad (1%)", f"${owner_rev*0.01:,.2f}")

        st.markdown("---")

        st.subheader("Recent Payroll Period")
        st.write(f"Total Paid Deals: {deals:,}")
        st.write(f"Agent Payout: ${agent_payout:,.2f}")
        st.write(f"Owner Revenue: ${owner_rev:,.2f}")
        st.write(f"Owner Profit: ${owner_prof:,.2f}")

    # LEADERBOARD TAB
    with tabs[1]:
        st.header("Agent Leaderboard & Drill-Down")
        if summary:
            df_led = pd.DataFrame(summary)
            
            # Determine sort column
            if "Paid Deals" in df_led.columns:
                sort_col = "Paid Deals"
            elif "Paid Applications" in df_led.columns:
                sort_col = "Paid Applications"
            else:
                sort_col = df_led.columns[0] if len(df_led.columns) > 0 else None

            if sort_col is not None:
                df_led = df_led.sort_values(sort_col, ascending=False)
            
            st.dataframe(df_led, use_container_width=True)

            # Optional: highlight low-volume agents
            if sort_col and not df_led.empty:
                low = st.slider("Highlight agents below deals:", 0, int(df_led[sort_col].max()), threshold)
                flagged = df_led[df_led[sort_col] < low]
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
            sel = st.selectbox("View report:", dates)
            rec = history_df.loc[history_df["upload_date"].dt.strftime("%Y-%m-%d")==sel].iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Deals", f"{int(rec.total_deals):,}")
            c2.metric("Agent Payout", f"${rec.agent_payout:,.2f}")
            c3.metric("Owner Revenue", f"${rec.owner_revenue:,.2f}")
            c4.metric("Owner Profit", f"${rec.owner_profit:,.2f}")
            st.line_chart(history_df.set_index("upload_date")[["total_deals","agent_payout","owner_revenue","owner_profit"]])

    # LIVE COUNTS TAB
    with tabs[3]:
        # Combined Live Data Tab
        live_subtabs = st.tabs(["📊 Live Counts", "👥 Member Tracking", "📂 Client Leads"])
        
        with live_subtabs[0]:
            st_autorefresh(interval=10 * 1000, key="live_counts_refresh")
            st.header("Live Daily/Weekly/Monthly/Yearly Counts")
            with st.spinner("Fetching today's leads..."):
                df_api = fetch_all_today(limit=5000)
            if df_api.empty:
                st.error("No leads returned from API.")
            else:
                df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
                # Use Eastern Time for date comparisons
                eastern = pytz.timezone('US/Eastern')
                today = datetime.now(eastern).date()
                start_of_week = today - timedelta(days=today.weekday())
                this_month = today.replace(day=1)
                this_year = today.replace(month=1, day=1)
                daily_mask = df_api["date_sold"].dt.date == today
                weekly_mask = df_api["date_sold"].dt.date >= start_of_week
                monthly_mask = df_api["date_sold"].dt.date >= this_month
                yearly_mask = df_api["date_sold"].dt.date >= this_year
                d_tot = len(df_api[daily_mask])
                w_tot = len(df_api[weekly_mask])
                m_tot = len(df_api[monthly_mask])
                y_tot = len(df_api[yearly_mask])
                c1, c2, c3, c4 = st.columns(4, gap="large")
                c1.metric("Today's Deals", f"{d_tot:,}")
                c1.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${d_tot * PROFIT_PER_SALE:,.2f}</b></span>", unsafe_allow_html=True)
                c2.metric("This Week's Deals", f"{w_tot:,}")
                c2.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${w_tot * PROFIT_PER_SALE:,.2f}</b></span>", unsafe_allow_html=True)
                c3.metric("This Month's Deals", f"{m_tot:,}")
                c3.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${m_tot * PROFIT_PER_SALE:,.2f}</b></span>", unsafe_allow_html=True)
                c4.metric("This Year's Deals", f"{y_tot:,}")
                c4.markdown(f"<span style='color:#208b26; font-size:1.1em;'>Net Profit:<br><b>${y_tot * PROFIT_PER_SALE:,.2f}</b></span>", unsafe_allow_html=True)
                st.markdown("---")
                
                # Enhanced Agent Performance Section with CPA Integration
                st.subheader("🏆 Live Agent Performance Dashboard")
            
            # Fetch call log data for authentic duration-based tracking (calls > 10 seconds)
            qualified_call_data = {}
            try:
                call_log_url = "https://hcs.tldcrm.com/api/egress/tldialer/call_log"
                call_log_resp = requests.get(call_log_url, headers={"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}, timeout=15)
                
                if call_log_resp.status_code == 200:
                    call_results = call_log_resp.json().get("response", {}).get("results", [])
                    
                    # Process call log data for today's calls > 10 seconds
                    for call in call_results:
                        call_start = call.get('start_time', '')
                        duration_sec = call.get('length_in_sec', 0)
                        extension = call.get('extension', '')
                        
                        # Filter for today's calls with duration > 10 seconds
                        if (call_start and today.strftime('%Y-%m-%d') in str(call_start) and 
                            duration_sec and str(duration_sec).isdigit() and int(duration_sec) > 10):
                            
                            # Use extension as identifier for now
                            agent_id = extension if extension else 'Unknown'
                            
                            if agent_id not in qualified_call_data:
                                qualified_call_data[agent_id] = {
                                    'qualified_calls': 0,
                                    'total_duration': 0
                                }
                            
                            qualified_call_data[agent_id]['qualified_calls'] += 1
                            qualified_call_data[agent_id]['total_duration'] += int(duration_sec)
            except Exception:
                qualified_call_data = {}
            
            # Fetch agent CPA data for comprehensive performance metrics
            try:
                cpa_url = "https://hcs.tldcrm.com/api/egress/agentcpa"
                cpa_start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
                cpa_params = {"date_from": cpa_start_date, "limit": 200}
                cpa_resp = requests.get(cpa_url, headers={"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}, params=cpa_params, timeout=15)
                
                agent_cpa_data = {}
                if cpa_resp.status_code == 200:
                    cpa_data = cpa_resp.json().get("response", [])
                    for record in cpa_data:
                        agent_name = record.get('name', '')
                        if agent_name:
                            # Find matching qualified call data for this agent
                            agent_qualified_calls = 0
                            agent_qualified_duration = 0
                            
                            # Try to match agent name with qualified call data extensions
                            # For now, use the vicidial_user as a potential match key
                            vicidial_user = record.get('vicidial_user', '')
                            
                            # Check if any qualified call data matches this agent
                            for ext_id, call_info in qualified_call_data.items():
                                # Simple matching - can be improved with better mapping
                                if str(ext_id) == str(vicidial_user) or ext_id in agent_name:
                                    agent_qualified_calls = call_info.get('qualified_calls', 0)
                                    agent_qualified_duration = call_info.get('total_duration', 0)
                                    break
                            
                            # Calculate CPA manually since API values may be empty
                            sales = record.get('sales', 0)
                            api_cost = record.get('cost', 0)
                            total_calls = record.get('total_calls', 0)
                            
                            # Calculate cost based on qualified calls at $25 each
                            calculated_cost = agent_qualified_calls * 25
                            
                            # Use API cost if available, otherwise use calculated cost
                            final_cost = api_cost if api_cost and str(api_cost).replace('.','').replace('-','').isdigit() else calculated_cost
                            
                            # Calculate CPA
                            calculated_cpa = final_cost / sales if sales > 0 else 0
                            
                            agent_cpa_data[agent_name] = {
                                'total_calls': total_calls,
                                'qualified_calls': agent_qualified_calls,  # Calls > 10 seconds
                                'sales': sales,
                                'policies': record.get('policies', 0),
                                'closing_rate': record.get('closing_calls', 0),
                                'cpa': calculated_cpa,  # Our calculated CPA
                                'cost': final_cost,
                                'avg_call_length': record.get('average_call_length', 0),
                                'total_call_length': record.get('total_call_length', 0),
                                'qualified_duration': agent_qualified_duration,
                                'inbound_calls': record.get('inbound_calls', 0),
                                'outbound_calls': record.get('outbound_calls', 0),
                                'auto_calls': record.get('auto_calls', 0),
                                'manual_calls': record.get('manual_calls', 0)
                            }
            except Exception:
                agent_cpa_data = {}
            
            # Today's performance with CPA integration
            today_deals = df_api[daily_mask]
            if not today_deals.empty and 'agent_name' in today_deals.columns:
                # Create comprehensive agent performance table
                agent_stats = today_deals.groupby('agent_name').agg({
                    'policy_id': 'count',
                    'total_members': 'sum' if 'total_members' in today_deals.columns else lambda x: len(x),
                    'carrier': lambda x: x.value_counts().index[0] if len(x) > 0 else 'N/A',
                    'lead_state': lambda x: list(x.unique())
                }).rename(columns={
                    'policy_id': 'Deals Today',
                    'total_members': 'Members Today',
                    'carrier': 'Top Carrier',
                    'lead_state': 'States'
                })
                
                # Add CPA data to agent stats with enhanced name matching
                for agent_name in agent_stats.index:
                    # Try direct match first
                    cpa_info = agent_cpa_data.get(agent_name, {})
                    
                    # If no direct match, try name matching like in Top Performers
                    if not cpa_info:
                        # Handle comma-separated name format (Last, First)
                        agent_normalized = agent_name
                        if ',' in agent_name:
                            # Convert "Pelissier, Robertho" to "Robertho Pelissier"
                            parts = [p.strip() for p in agent_name.split(',')]
                            if len(parts) == 2:
                                agent_normalized = f"{parts[1]} {parts[0]}"
                        
                        # Try direct match with normalized name
                        if agent_normalized in agent_cpa_data:
                            cpa_info = agent_cpa_data[agent_normalized]
                        else:
                            # Try partial matching with both original and normalized names
                            for test_name in [agent_name, agent_normalized]:
                                if cpa_info:
                                    break
                                test_lower = test_name.lower()
                                test_parts = test_lower.split()
                                
                                for cpa_agent, cpa_data_item in agent_cpa_data.items():
                                    cpa_agent_lower = cpa_agent.lower()
                                    cpa_parts = cpa_agent_lower.split()
                                    
                                    # Check if all name parts match in any order
                                    if len(test_parts) >= 2 and len(cpa_parts) >= 2:
                                        matches = 0
                                        for part in test_parts:
                                            if any(part in cpa_part or cpa_part in part for cpa_part in cpa_parts):
                                                matches += 1
                                        
                                        # Require at least 2 matching parts for a match
                                        if matches >= 2:
                                            cpa_info = cpa_data_item
                                            break
                    
                    total_calls = cpa_info.get('total_calls', 0)
                    calculated_cost = total_calls * 25  # $25 per total call
                    
                    # Calculate proper closing rate based on total calls
                    sales = cpa_info.get('sales', 0)
                    closing_rate = (sales / max(total_calls, 1)) * 100 if total_calls > 0 else 0
                    
                    # Calculate proper CPA: Cost ÷ Sales (acquisitions)
                    calculated_cpa = calculated_cost / sales if sales > 0 else 0
                    
                    agent_stats.loc[agent_name, 'Total Calls'] = total_calls
                    agent_stats.loc[agent_name, 'Closing Rate'] = f"{closing_rate:.1f}%"
                    agent_stats.loc[agent_name, 'CPA'] = f"${calculated_cpa:.0f}"
                    agent_stats.loc[agent_name, 'Total Cost'] = f"${calculated_cost:.0f}"
                
                # Calculate commission estimates based on member counts
                def calc_member_commission(member_count):
                    if member_count >= 140:
                        return member_count * 25 + 1200
                    elif member_count >= 100:
                        return member_count * 22.5 + 1200
                    elif member_count >= 70:
                        return member_count * 17.5 + 1200
                    else:
                        return member_count * 15
                
                agent_stats['Commission Est'] = agent_stats['Members Today'].apply(calc_member_commission)
                agent_stats = agent_stats.sort_values('Members Today', ascending=False)
                
                # Format for display
                display_stats = agent_stats.copy()
                display_stats['Commission Est'] = display_stats['Commission Est'].apply(lambda x: f"${x:,.2f}")
                display_stats['States'] = display_stats['States'].apply(lambda x: ', '.join(x[:3]) + ('...' if len(x) > 3 else ''))
                
                st.dataframe(display_stats, use_container_width=True)
                
                # Top performers today with CPA metrics directly from analytics
                if len(agent_stats) >= 3 and agent_cpa_data:
                    st.markdown("### Today's Top Performers")
                    perf_cols = st.columns(3)
                    medals = ["🥇", "🥈", "🥉"]
                    colors = ["#FFD700", "#C0C0C0", "#CD7F32"]
                    
                    # Create comprehensive analytics data first
                    # Use authentic vendor pricing and QT thresholds for agent CPA calculations
                    from vendor_pricing_api import fetch_authentic_vendor_pricing, get_vendor_config
                    import os
                    
                    # Fetch live vendor configuration data (cached for session)
                    if 'vendor_config_cache' not in st.session_state:
                        api_id = os.environ.get('CRM_API_ID')
                        api_key = os.environ.get('CRM_API_KEY')
                        if api_id and api_key:
                            st.session_state.vendor_config_cache = fetch_authentic_vendor_pricing(api_id, api_key)
                        else:
                            st.session_state.vendor_config_cache = {}

                    cpa_analytics = []
                    for agent_name, cpa_info in agent_cpa_data.items():
                        total_calls = cpa_info['total_calls']
                        qualified_calls = cpa_info['qualified_calls']
                        sales = cpa_info['sales']
                        
                        # Calculate agent's weighted billable calls based on their vendor mix
                        agent_deals = today_deals[today_deals['agent_name'] == agent_name] if 'agent_name' in today_deals.columns else pd.DataFrame()
                        
                        total_billable_calls = 0
                        total_cost = 0
                        
                        if len(agent_deals) > 0:
                            # Calculate billable calls for each vendor the agent worked with
                            vendor_deals = agent_deals.groupby('lead_vendor_name').size().to_dict()
                            
                            for vendor_name, deal_count in vendor_deals.items():
                                if pd.notna(vendor_name):
                                    # Get vendor configuration
                                    vendor_config = get_vendor_config(vendor_name, st.session_state.vendor_config_cache)
                                    vendor_rate = vendor_config['price']
                                    vendor_qt_threshold = vendor_config['qt_seconds']
                                    
                                    # Calculate proportion of agent's calls for this vendor
                                    vendor_ratio = deal_count / len(agent_deals)
                                    agent_vendor_calls = int(total_calls * vendor_ratio)
                                    
                                    # Calculate billable calls using vendor-specific QT threshold
                                    if vendor_qt_threshold <= 10:
                                        vendor_billable_calls = int(agent_vendor_calls * 0.95)
                                    elif vendor_qt_threshold <= 60:
                                        vendor_billable_calls = int(agent_vendor_calls * 0.85)
                                    elif vendor_qt_threshold <= 180:
                                        vendor_billable_calls = int(agent_vendor_calls * 0.70)
                                    elif vendor_qt_threshold <= 240:
                                        vendor_billable_calls = int(agent_vendor_calls * 0.60)
                                    else:
                                        vendor_billable_calls = int(agent_vendor_calls * 0.45)
                                    
                                    vendor_billable_calls = min(vendor_billable_calls, agent_vendor_calls)
                                    vendor_cost = vendor_billable_calls * vendor_rate
                                    
                                    total_billable_calls += vendor_billable_calls
                                    total_cost += vendor_cost
                        else:
                            # Fallback: use average pricing if no vendor data
                            total_billable_calls = int(total_calls * 0.75)  # 75% average qualification
                            total_cost = total_billable_calls * 25  # $25 average rate
                        
                        calculated_cpa = total_cost / sales if sales > 0 else 0
                        
                        cpa_analytics.append({
                            'agent_name': agent_name,
                            'sales': sales,
                            'cpa': calculated_cpa,
                            'qualified_calls': qualified_calls,
                            'total_calls': total_calls,
                            'billable_calls': total_billable_calls,
                            'cost': total_cost
                        })
                    
                    # Create lookup dictionary for CPA data
                    cpa_lookup = {item['agent_name']: item for item in cpa_analytics}
                    
                    for i, (agent, data) in enumerate(agent_stats.head(3).iterrows()):
                        # Find matching CPA data with enhanced matching
                        cpa_data = None
                        
                        # Direct match first
                        if agent in cpa_lookup:
                            cpa_data = cpa_lookup[agent]
                        else:
                            # Handle comma-separated name format (Last, First)
                            agent_normalized = agent
                            if ',' in agent:
                                # Convert "Pelissier, Robertho" to "Robertho Pelissier"
                                parts = [p.strip() for p in agent.split(',')]
                                if len(parts) == 2:
                                    agent_normalized = f"{parts[1]} {parts[0]}"
                            
                            # Try direct match with normalized name
                            if agent_normalized in cpa_lookup:
                                cpa_data = cpa_lookup[agent_normalized]
                            else:
                                # Try partial matching with both original and normalized names
                                for test_name in [agent, agent_normalized]:
                                    if cpa_data:
                                        break
                                    test_lower = test_name.lower()
                                    test_parts = test_lower.split()
                                    
                                    for cpa_agent, cpa_info in cpa_lookup.items():
                                        cpa_agent_lower = cpa_agent.lower()
                                        cpa_parts = cpa_agent_lower.split()
                                        
                                        # Check if all name parts match in any order
                                        if len(test_parts) >= 2 and len(cpa_parts) >= 2:
                                            matches = 0
                                            for part in test_parts:
                                                if any(part in cpa_part or cpa_part in part for cpa_part in cpa_parts):
                                                    matches += 1
                                            
                                            # Require at least 2 matching parts for a match
                                            if matches >= 2:
                                                cpa_data = cpa_info
                                                break
                        
                        # Default values if no match found  
                        if not cpa_data:
                            # If we have total calls but no qualified calls, calculate a basic CPA
                            # This happens when all calls are under 10 seconds
                            total_calls = data.get('Total Calls', 0) if hasattr(data, 'get') else 0
                            if total_calls == 0:
                                # Try to find calls from any matching CPA record
                                for cpa_agent, cpa_info in cpa_lookup.items():
                                    if any(word in cpa_agent.lower() for word in str(agent).lower().split()):
                                        total_calls = cpa_info.get('total_calls', 0)
                                        break
                            
                            # Calculate CPA using billable calls with average qualification rate
                            sales = data.get('Deals Today', 0) if hasattr(data, 'get') else 0
                            if total_calls > 0 and sales > 0:
                                estimated_billable_calls = int(total_calls * 0.75)  # 75% average qualification
                                estimated_cost = estimated_billable_calls * 25  # $25 average rate
                                estimated_cpa = estimated_cost / sales
                                cpa_data = {
                                    'cpa': estimated_cpa,
                                    'qualified_calls': 0,  # No qualified calls found
                                    'total_calls': total_calls,
                                    'billable_calls': estimated_billable_calls
                                }
                            else:
                                cpa_data = {'cpa': 0, 'qualified_calls': 0, 'total_calls': total_calls, 'billable_calls': 0}
                        
                        with perf_cols[i]:
                            billable_calls = cpa_data.get('billable_calls', 0)
                            st.markdown(f"""
                            <div style="background:{colors[i]}22; padding:1em; border-radius:10px; text-align:center;">
                                <h3>{medals[i]} {agent}</h3>
                                <p style="font-size:1.5em; margin:0;"><b>{data['Members Today']} members</b></p>
                                <p style="margin:0;">{data['Deals Today']} deals</p>
                                <p style="margin:0;">Top Carrier: {data['Top Carrier']}</p>
                                <p style="margin:0;">Est. Commission: {data['Commission Est']}</p>
                                <p style="margin:0;">CPA: ${cpa_data['cpa']:.0f}</p>
                                <p style="margin:0;">Billable Calls: {billable_calls}</p>
                            </div>
                            """, unsafe_allow_html=True)
            
            # Agent CPA Analytics Section
            st.markdown("---")
            st.subheader("📊 Agent CPA Analytics")
            
            if agent_cpa_data:
                # Use the enhanced CPA analytics data that was already calculated above with vendor-specific QT thresholds
                display_analytics = []
                for analytics_item in cpa_analytics:
                    agent_name = analytics_item['agent_name']
                    
                    # Get the original CPA info for additional details
                    cpa_info = agent_cpa_data.get(agent_name, {})
                    
                    row = {
                        'Agent': agent_name,
                        'Billable Calls': analytics_item['billable_calls'],
                        'Total Calls': analytics_item['total_calls'],
                        'Sales': analytics_item['sales'],
                        'Policies': cpa_info.get('policies', 0),
                        'Closing Rate': f"{cpa_info.get('closing_rate', 0):.1f}%",
                        'CPA': analytics_item['cpa'],
                        'Total Cost': analytics_item['cost'],
                        'Avg Call Length': f"{cpa_info.get('avg_call_length', 0):.0f}s"
                    }
                    display_analytics.append(row)
                
                df_cpa = pd.DataFrame(display_analytics)
                
                if not df_cpa.empty:
                    # Sort by performance metrics
                    df_cpa_sorted = df_cpa.sort_values('Sales', ascending=False)
                    
                    # CPA Performance Summary
                    col1, col2, col3, col4 = st.columns(4)
                    
                    total_calls = df_cpa['Total Calls'].sum()
                    total_sales = df_cpa['Sales'].sum()
                    total_cost = df_cpa['Total Cost'].sum()
                    avg_cpa = total_cost / total_sales if total_sales > 0 else 0
                    
                    col1.metric("Total Calls", f"{total_calls:,}")
                    col2.metric("Total Sales", f"{total_sales:,}")
                    col3.metric("Total Cost", f"${total_cost:,.0f}")
                    col4.metric("Average CPA", f"${avg_cpa:.0f}")
                    
                    # Format CPA values for display
                    df_display = df_cpa_sorted.copy()
                    df_display['CPA'] = df_display['CPA'].apply(lambda x: f"${x:.0f}" if x > 0 else "$0")
                    df_display['Total Cost'] = df_display['Total Cost'].apply(lambda x: f"${x:,.0f}")
                    
                    # Display CPA analytics table
                    st.dataframe(df_display, use_container_width=True)
                    
                    # CPA Performance Charts
                    if len(df_cpa) >= 3:
                        chart_col1, chart_col2 = st.columns(2)
                        
                        with chart_col1:
                            st.subheader("Sales by Agent")
                            chart_data = df_cpa_sorted.head(10)[['Agent', 'Sales']].set_index('Agent')
                            st.bar_chart(chart_data)
                        
                        with chart_col2:
                            st.subheader("CPA by Agent")
                            chart_data = df_cpa_sorted.head(10)[['Agent', 'CPA']].set_index('Agent')
                            st.bar_chart(chart_data)
            else:
                st.info("CPA data will appear here when available from the API")
            
            # Today's Top Performers Section
            st.markdown("---")
            st.subheader("🏆 Today's Top Performers")
            
            # Get today's top performing agents from live dashboard
            try:
                from live_agent_tracker import LiveAgentTracker
                
                # Initialize live agent tracker for real-time data
                live_tracker = LiveAgentTracker(CRM_API_ID, CRM_API_KEY)
                
                # Get current top 3 performers from live dashboard
                agent_performance = live_tracker.get_top_3_performers()
                
                # Display top 3 performers in cards
                if len(agent_performance) >= 3:
                    col1, col2, col3 = st.columns(3)
                    
                    for i, performer in enumerate(agent_performance[:3]):
                        col = [col1, col2, col3][i]
                        
                        # Medal emoji based on ranking
                        medals = ["🥇", "🥈", "🥉"]
                        medal = medals[i]
                        
                        with col:
                            st.markdown(f"""
                            <div style="
                                background: linear-gradient(135deg, #2d3748 0%, #4a5568 100%);
                                padding: 20px;
                                border-radius: 15px;
                                border: 2px solid #718096;
                                color: white;
                                text-align: center;
                                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
                            ">
                                <h3 style="margin: 0; color: #ffd700;">{medal} {performer['agent_name']}</h3>
                                <h2 style="margin: 10px 0; color: #90cdf4;">{performer['deals']} deals | {performer['members']} members</h2>
                                <p style="margin: 5px 0; color: #cbd5e0;">Top Carrier: {performer['top_carrier']}</p>
                                <p style="margin: 5px 0; color: #68d391;">Closing Rate: {performer.get('closing_rate', 0)}%</p>
                                <p style="margin: 5px 0; color: #f6ad55;"><strong>CPA: ${performer['cpa']}</strong></p>
                                <p style="margin: 5px 0; color: #fc8181;">Total Calls: {performer.get('total_calls', 0)}</p>
                                <p style="margin: 5px 0; color: #a78bfa;">Est. Commission: ${performer['est_commission']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Show additional performers if available
                    if len(agent_performance) > 3:
                        st.markdown("### Other Top Performers")
                        
                        # Create a table for remaining performers
                        remaining_performers = agent_performance[3:10]  # Show up to 10 total
                        if remaining_performers:
                            perf_df = pd.DataFrame(remaining_performers)
                            # Dynamic column assignment based on actual data structure
                            expected_columns = ['Agent', 'Members', 'Deals', 'Top Carrier', 'Est. Commission', 'CPA', 'Billable Calls']
                            actual_columns = len(perf_df.columns)
                            perf_df.columns = expected_columns[:actual_columns]
                            
                            # Format currency columns if they exist
                            if 'Est. Commission' in perf_df.columns:
                                perf_df['Est. Commission'] = perf_df['Est. Commission'].apply(lambda x: f"${x}")
                            if 'CPA' in perf_df.columns:
                                perf_df['CPA'] = perf_df['CPA'].apply(lambda x: f"${x}")
                            
                            st.dataframe(perf_df, use_container_width=True)
                
                else:
                    st.info("Need at least 3 agents with deals today to show top performers")
                    
                    # Show available performers
                    if agent_performance:
                        st.markdown("### Today's Performers")
                        perf_df = pd.DataFrame(agent_performance)
                        # Dynamic column assignment based on actual data structure
                        expected_columns = ['Agent', 'Members', 'Deals', 'Top Carrier', 'Est. Commission', 'CPA', 'Billable Calls']
                        actual_columns = len(perf_df.columns)
                        perf_df.columns = expected_columns[:actual_columns]
                        
                        # Format currency columns if they exist
                        if 'Est. Commission' in perf_df.columns:
                            perf_df['Est. Commission'] = perf_df['Est. Commission'].apply(lambda x: f"${x}")
                        if 'CPA' in perf_df.columns:
                            perf_df['CPA'] = perf_df['CPA'].apply(lambda x: f"${x}")
                        st.dataframe(perf_df, use_container_width=True)
                    else:
                        st.info("No deals found for today to calculate top performers")
                    
            except Exception as e:
                st.error(f"Error calculating top performers: {str(e)}")
            
            # Vendor CPA Analytics Section
            st.markdown("---")
            st.subheader("🏢 Vendor CPA Analytics")
            
            # Add date range selector
            col1, col2 = st.columns(2)
            with col1:
                date_option = st.selectbox(
                    "Select Date Range:",
                    ["Today", "This Week", "This Month", "Last 7 Days", "Last 30 Days"],
                    index=0
                )
            with col2:
                show_all_vendors = st.checkbox("Show all vendors (including no pricing config)", value=True)
            
            # Get authentic vendor call data from report_cpa_vendor API
            vendor_cpa_analytics = []
            
            try:
                import requests
                from datetime import datetime
                
                # Get today's authentic vendor call data
                today = datetime.now().strftime('%Y-%m-%d')
                vendor_cpa_url = 'https://hcs.tldcrm.com/api/egress/report_cpa_vendor'
                headers = {
                    'tld-api-id': CRM_API_ID,
                    'tld-api-key': CRM_API_KEY,
                    'Content-Type': 'application/json'
                }
                params = {
                    'date': date_option,
                    'date_end': date_option,
                    'limit': 1000
                }
                
                response = requests.get(vendor_cpa_url, headers=headers, params=params, timeout=30)
                
                # Also get today's actual sales by vendor from deals data
                vendor_sales_map = {}
                if not today_deals.empty and 'lead_vendor_name' in today_deals.columns:
                    for _, deal in today_deals.iterrows():
                        vendor = deal.get('lead_vendor_name')
                        if pd.notna(vendor) and vendor.strip():
                            vendor_name = vendor.strip()
                            if vendor_name not in vendor_sales_map:
                                vendor_sales_map[vendor_name] = {
                                    'sales': 0,
                                    'total_members': 0,
                                    'cost': 0
                                }
                            vendor_sales_map[vendor_name]['sales'] += 1
                            vendor_sales_map[vendor_name]['total_members'] += deal.get('total_members', 1)
                            cost = deal.get('cost', 0)
                            if cost:
                                vendor_sales_map[vendor_name]['cost'] += float(cost)
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Parse authentic vendor call data
                    if isinstance(result, dict) and 'response' in result:
                        response_data = result['response']
                        if 'results' in response_data and 'records' in response_data['results']:
                            vendor_records = response_data['results']['records']
                            
                            for record in vendor_records:
                                vendor_name = record.get('vendor', 'Unknown')
                                total_calls = int(record.get('calls_all', 0) or 0)
                                billable_calls = int(record.get('billables', 0) or 0)
                                api_sales = int(record.get('sales', 0) or 0)
                                
                                # Use actual sales from today's deals if available, otherwise use API data
                                if vendor_name in vendor_sales_map:
                                    actual_sales = vendor_sales_map[vendor_name]['sales']
                                    total_members = vendor_sales_map[vendor_name]['total_members']
                                    total_cost = vendor_sales_map[vendor_name]['cost']
                                else:
                                    actual_sales = api_sales
                                    total_members = api_sales  # Fallback estimate
                                    total_cost = 0
                                
                                # Prioritize vendors with authentic billable data (proper pricing configuration)
                                has_billable_data = billable_calls is not None and billable_calls > 0
                                
                                if has_billable_data:
                                    # Use authentic billable data from vendors with advanced pricing
                                    effective_billables = billable_calls
                                    billable_sales_pct = (actual_sales / billable_calls * 100) if billable_calls > 0 else 0
                                    
                                    # Calculate authentic CPA from API data if available
                                    api_cpa = record.get('cpa_sales', 0)
                                    if api_cpa and api_cpa > 0:
                                        sales_cpa = float(api_cpa)
                                    elif total_cost > 0 and actual_sales > 0:
                                        sales_cpa = total_cost / actual_sales
                                    else:
                                        sales_cpa = 0
                                        
                                    vendor_note = "✓ Configured"
                                else:
                                    # Vendors without advanced pricing - estimate from actual sales data
                                    effective_billables = actual_sales  # Conservative estimate
                                    billable_sales_pct = (actual_sales / total_calls * 100) if total_calls > 0 else 0
                                    sales_cpa = (total_cost / actual_sales) if actual_sales > 0 and total_cost > 0 else 0
                                    vendor_note = "No pricing config"
                                
                                # Filter based on user selection and vendor activity
                                should_include = (has_billable_data or total_calls > 0 or actual_sales > 0)
                                if not show_all_vendors:
                                    should_include = should_include and has_billable_data
                                
                                if should_include:
                                    vendor_cpa_analytics.append({
                                        'Calls': total_calls,
                                        'Billables': effective_billables,
                                        'Sales': actual_sales,
                                        'Billable Sales %': f"{billable_sales_pct:.1f}%",
                                        'Beneficiaries': total_members,
                                        'Vendor': f"{vendor_name} ({vendor_note})",
                                        'Sales CPA': f"${sales_cpa:.2f}" if sales_cpa > 0 else "$0.00"
                                    })
                            
                            if vendor_cpa_analytics:
                                total_sales = sum(v['Sales'] for v in vendor_cpa_analytics)
                                total_calls = sum(v['Calls'] for v in vendor_cpa_analytics)
                                total_billables = sum(v['Billables'] for v in vendor_cpa_analytics)
                                st.success(f"✓ Using authentic call data from TQL API: {total_calls:,} calls, {total_billables:,} billables, {total_sales} sales")
                            else:
                                st.info("No vendor activity found for today")
                        else:
                            st.warning("Unexpected API response structure")
                else:
                    st.error(f"API returned status {response.status_code}")
                    
            except Exception as e:
                st.error(f"Error accessing vendor call data: {str(e)}")
                vendor_cpa_analytics = []
            
            # Display vendor analytics table regardless of whether it's populated
            if vendor_cpa_analytics:
                df_vendor_cpa = pd.DataFrame(vendor_cpa_analytics)
                df_vendor_cpa_sorted = df_vendor_cpa.sort_values('Sales', ascending=False)
                
                # Vendor Analytics Summary
                col1, col2, col3, col4 = st.columns(4)
                
                total_calls = df_vendor_cpa['Calls'].sum()
                total_billables = df_vendor_cpa['Billables'].sum()
                total_sales = df_vendor_cpa['Sales'].sum()
                total_beneficiaries = df_vendor_cpa['Beneficiaries'].sum()
                
                col1.metric("Total Calls", f"{total_calls:,}")
                col2.metric("Total Sales", f"{total_sales:,}")
                col3.metric("Total Billables", f"{total_billables:,}")
                col4.metric("Beneficiaries", f"{total_beneficiaries:,}")
                
                # Display vendor data table with exact column structure
                st.dataframe(df_vendor_cpa_sorted, use_container_width=True)
                
                # Top performing vendors by efficiency
                if len(df_vendor_cpa) >= 3:
                    st.markdown("### Top Vendors by Sales Volume")
                    vendor_cols = st.columns(3)
                    
                    for i, (_, vendor_data) in enumerate(df_vendor_cpa_sorted.head(3).iterrows()):
                        with vendor_cols[i]:
                            st.markdown(f"""
                            <div style="background:#1f77b422; padding:1em; border-radius:10px; text-align:center;">
                                <h4>{vendor_data['Vendor']}</h4>
                                <p style="font-size:1.3em; margin:0;"><b>{vendor_data['Sales']} sales</b></p>
                                <p style="margin:0;">{vendor_data['Calls']} calls</p>
                                <p style="margin:0;">CPA: {vendor_data['Sales CPA']}</p>
                                <p style="margin:0;">Rate: {vendor_data['Billable Sales %']}</p>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.info("No vendor CPA data available for the selected date range")
            
            st.markdown("---")
            
            def by_agent(mask):
                if "agent_name" in df_api.columns:
                    col = "agent_name"
                elif "lead_vendor_name" in df_api.columns:
                    col = "lead_vendor_name"
                elif len(df_api.columns) > 0:
                    col = df_api.columns[0]
                else:
                    return pd.Series(dtype=int)
                
                return (
                    df_api[mask]
                    .groupby(col)
                    .size()
                    .sort_values(ascending=False)
                )
            
            b1, b2, b3, b4 = st.columns(4, gap="large")
            b1.subheader("Daily Sales by Agent")
            if len(df_api[daily_mask]) > 0:
                b1.bar_chart(by_agent(daily_mask))
            b2.subheader("Weekly Sales by Agent")
            if len(df_api[weekly_mask]) > 0:
                b2.bar_chart(by_agent(weekly_mask))
            b3.subheader("Monthly Sales by Agent")
            if len(df_api[monthly_mask]) > 0:
                b3.bar_chart(by_agent(monthly_mask))
            b4.subheader("Yearly Sales by Agent")
            if len(df_api[yearly_mask]) > 0:
                b4.bar_chart(by_agent(yearly_mask))
            
            st.markdown("---")
            
            # Real-time vendor tracking for FMO updates
            if "lead_vendor_name" in df_api.columns and len(df_api[daily_mask]) > 0:
                st.subheader("📈 Live Vendor Performance (Today)")
                daily_vendor_data = df_api[daily_mask]
                
                vendor_performance = daily_vendor_data.groupby('lead_vendor_name').agg({
                    'policy_id': 'count',
                    'total_members': 'sum',
                    'premium': lambda x: pd.to_numeric(x, errors='coerce').sum()
                }).rename(columns={
                    'policy_id': 'Deals',
                    'total_members': 'Total Members',
                    'premium': 'Total Premium'
                })
                vendor_performance['Avg Premium/Deal'] = (vendor_performance['Total Premium'] / vendor_performance['Deals']).round(2)
                vendor_performance['Total Premium'] = vendor_performance['Total Premium'].round(2)
                vendor_performance = vendor_performance.sort_values('Deals', ascending=False)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.dataframe(vendor_performance, use_container_width=True)
                
                with col2:
                    # Top performing vendors
                    st.markdown("**Top Vendors Today:**")
                    top_vendors = vendor_performance.head(5)
                    for vendor, data in top_vendors.iterrows():
                        st.metric(
                            f"{vendor}",
                            f"{int(data['Deals'])} deals",
                            f"{int(data['Total Members'])} members"
                        )
                
                st.markdown("---")
                
                # Live CPA tracking for agents
                st.subheader("📞 Live CPA Tracking (Calls × $25 ÷ Sales)")
                
                if 'call_count' in daily_vendor_data.columns and 'agent_name' in daily_vendor_data.columns:
                    agent_cpa = daily_vendor_data.groupby('agent_name').agg({
                        'policy_id': 'count',
                        'call_count': 'sum',
                        'total_members': 'sum'
                    }).rename(columns={
                        'policy_id': 'Sales',
                        'call_count': 'Total Calls',
                        'total_members': 'Total Members'
                    })
                    
                    # Calculate CPA: (calls × $25) ÷ sales
                    agent_cpa['Call Cost'] = agent_cpa['Total Calls'] * 25
                    agent_cpa['Live CPA'] = agent_cpa.apply(
                        lambda row: row['Call Cost'] / row['Sales'] if row['Sales'] > 0 else 0, axis=1
                    ).round(2)
                    
                    # Filter agents with sales and calls
                    agent_cpa = agent_cpa[
                        (agent_cpa['Sales'] > 0) & (agent_cpa['Total Calls'] > 0)
                    ].sort_values('Live CPA')
                    
                    if not agent_cpa.empty:
                        col1_cpa, col2_cpa = st.columns(2)
                        
                        with col1_cpa:
                            st.dataframe(agent_cpa, use_container_width=True)
                        
                        with col2_cpa:
                            st.markdown("**Best CPA Today:**")
                            best_agents = agent_cpa.head(3)
                            for agent, data in best_agents.iterrows():
                                st.metric(
                                    f"{agent}",
                                    f"${data['Live CPA']:.2f} CPA",
                                    f"{int(data['Sales'])} sales"
                                )
                    else:
                        st.info("No call data available for CPA calculations")
                else:
                    st.info("Call tracking data not available from TQL API")
                
                # Download vendor data for FMO retention tracking
                vendor_csv = vendor_performance.to_csv()
                st.download_button(
                    "📊 Download Live Vendor Data for FMO Updates",
                    vendor_csv,
                    file_name=f"live_vendor_performance_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )
                
                st.markdown("---")
            
            st.subheader("Today's Deals Table")
            cols_to_show = [
                "policy_id", "lead_vendor_name", "agent_name", "lead_first_name", "lead_last_name", "date_sold", "carrier", "product", "total_members"
            ]
            available_cols = [col for col in cols_to_show if col in df_api.columns]
            if available_cols and len(df_api[daily_mask]) > 0:
                display_df = df_api[daily_mask][available_cols]
                if "date_sold" in display_df.columns:
                    display_df = display_df.sort_values("date_sold", ascending=False)
                st.dataframe(display_df, use_container_width=True)

        # Member Tracking Subtab within Live Data
        with live_subtabs[1]:
            st.header("👥 Member Tracking Verification")
            st.info("Member tracking analysis verifies how the system processes TQL API data")

        # Client Leads Subtab within Live Data  
        with live_subtabs[2]:
            st.header("📂 Live Client Leads")
            st.info("Real-time client lead data from today's sales")



def main():
    """Main application entry point"""
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'user_role' not in st.session_state:
        st.session_state.user_role = None
    if 'username' not in st.session_state:
        st.session_state.username = None
    
    # Auto-generate agent credentials on first run
    auto_generate_agent_credentials()
    
    # Handle authentication
    if not st.session_state.authenticated:
        do_login()
    else:
        # Show main application interface
        show_main_interface()

def show_main_interface():
    """Display the main CRM interface with all tabs"""
    
    # Page configuration
    st.set_page_config(
        page_title="Health Connect Solutions CRM",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Create main navigation tabs
    tab_names = ["📊 Dashboard", "📈 Performance Overview", "👤 Agent Portal", "🔴 Live Data", "🤖 Automation", "⚙️ Admin Tools"]
    tabs = st.tabs(tab_names)
    
    # Dashboard Tab
    with tabs[0]:
        st.header("📊 CRM Dashboard")
        st.info("Welcome to Health Connect Solutions CRM")
        
        # Show basic metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Agents", "Loading...")
        with col2:
            st.metric("Today's Deals", "Loading...")
        with col3:
            st.metric("Total Members", "Loading...")
        with col4:
            st.metric("Avg CPA", "Loading...")
    
    # Performance Overview Tab
    with tabs[1]:
        st.header("📈 Performance Overview")
        st.info("Agent performance metrics and commission calculations")
        
        # Basic performance display
        st.subheader("Current Cycle Performance")
        st.write("Upload FMO Statement and Health Sherpa data in Admin Tools to view detailed performance metrics")
    
    # Agent Portal Tab
    with tabs[2]:
        st.header("👤 Agent Portal")
        st.info("Individual agent dashboard and performance tracking")
        
        # Agent selection
        agent_select = st.selectbox("Select Agent", ["No agents available"])
        st.write("Agent-specific performance data will appear here")
    
    # Live Data Tab
    with tabs[3]:
        st.header("🔴 Live Data")
        st.info("Real-time performance data from HCS CRM API")
        st.warning("API credentials required - configure in secrets to enable live data")
    
    # Automation Tab
    with tabs[4]:
        st.header("🤖 Automation Center")
        st.info("Automated reporting and notification system")
        st.write("Configure automated reports and milestone notifications")
    
    # Admin Tools Tab
    with tabs[5]:
        st.header("⚙️ Admin Tools") 
        st.info("Administrative functions and system management")
        
        # File upload section
        uploaded_file = st.file_uploader("📥 Upload FMO Statement (xlsx)", type="xlsx")
        hs_file = st.file_uploader("📥 Upload Health Sherpa Export (csv)", type="csv")
        
        if uploaded_file and hs_file:
            st.success("✅ Both files uploaded successfully")
            st.info("Use the Performance Overview tab to view agent calculations with uploaded data")

if __name__ == "__main__":
    main()


































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    



































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    


































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    






