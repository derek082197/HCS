import streamlit as st
import pandas as pd
import sqlite3
import io
import zipfile
import csv
from datetime import date, datetime, timedelta
from fpdf import FPDF
import requests

# 1) PAGE CONFIG
st.set_page_config(page_title="HCS Commission CRM", layout="wide")

# ── USER AUTH
@st.cache_data
def load_users():
    df = pd.read_csv("users.csv", dtype=str).dropna()
    return dict(zip(df.username.str.strip(), df.password))
USERS = load_users()
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def do_login():
    u, p = st.session_state.user.strip(), st.session_state.pwd
    if USERS.get(u) == p:
        st.session_state.logged_in = True
        st.success(f"✅ Welcome, {u}!")
    else:
        st.error("❌ Incorrect credentials")

def do_logout():
    st.session_state.logged_in = False
    st.experimental_rerun()

if not st.session_state.logged_in:
    with st.sidebar:
        st.title("🔒 HCS CRM Login")
        st.text_input("Username", key="user")
        st.text_input("Password", type="password", key="pwd")
        st.button("Log in", on_click=do_login)
    st.stop()

st.sidebar.button("Log out", on_click=do_logout)

# ── CONFIG
PROFIT_PER_SALE = 43.3
CRM_API_URL     = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID      = "310"
CRM_API_KEY     = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"
DB              = "crm_history.db"

# ── DB HELPERS
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
    conn.commit(); conn.close()

@st.cache_data
def load_history():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT * FROM reports ORDER BY upload_date", conn, parse_dates=["upload_date"])
    conn.close()
    df["total_deals"]   = pd.to_numeric(df["total_deals"], errors="coerce").fillna(0).astype(int)
    df["agent_payout"]  = pd.to_numeric(df["agent_payout"], errors="coerce").fillna(0.0)
    df["owner_revenue"] = pd.to_numeric(df["owner_revenue"], errors="coerce").fillna(0.0)
    df["owner_profit"]  = pd.to_numeric(df["owner_profit"], errors="coerce").fillna(0.0)
    return df

def insert_report(dt, totals):
    conn = sqlite3.connect(DB)
    conn.execute(
        "INSERT OR REPLACE INTO reports " +
        "(upload_date,total_deals,agent_payout,owner_revenue,owner_profit) VALUES (?,?,?,?,?)",
        (dt, totals['deals'], totals['agent'], totals['owner_rev'], totals['owner_prof'])
    )
    conn.commit(); conn.close()

# ── PDF
class PDF(FPDF): pass

def generate_agent_pdf(df_agent, agent_name):
    pdf = PDF()
    pdf.add_page(); pdf.set_font("Arial","B",16)
    pdf.cell(0,10,"Health Connect Solutions", ln=True, align="C")
    pdf.ln(5); pdf.set_font("Arial","B",12)
    pdf.cell(0,10,f"Commission Statement - {agent_name}", ln=True)
    pdf.ln(5)
    paid = df_agent[df_agent['Paid Status']=='Paid']
    paid_ct = len(paid)
    rate = 25 if paid_ct>=200 else 22.5 if paid_ct>=150 else 17.5 if paid_ct>=120 else 15
    bonus, payout = (1200, paid_ct*rate+1200) if paid_ct>=70 else (0, paid_ct*rate)
    pdf.set_font("Arial","",12)
    pdf.multi_cell(0,6, f"Deals: {len(df_agent)}, Paid: {paid_ct}, Rate: ${rate}, Bonus: ${bonus}, Payout: ${payout}")
    return pdf.output(dest='S').encode('latin1')

# ── API HELPERS
@st.cache_data(ttl=60)
def fetch_today_leads():
    headers = {"tld-api-id":CRM_API_ID, "tld-api-key":CRM_API_KEY}
    params  = {"date_from":date.today().strftime("%Y-%m-%d")}
    all_results, url, seen = [], CRM_API_URL, set()
    while url and url not in seen:
        seen.add(url)
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        js = r.json().get('response', {})
        chunk = js.get('results', [])
        if not chunk: break
        all_results.extend(chunk)
        nxt = js.get('navigate', {}).get('next')
        if not nxt or nxt in seen: break
        url = nxt; params={}
    return pd.DataFrame(all_results)

# ── INIT
init_db(); history_df = load_history(); summary=[]; uploaded_file=None; threshold=10

# ── TABS
tabs = st.tabs(["🏆 Overview","📋 Leaderboard","📈 History","📊 Live Counts","⚙️ Settings","📂 Clients"])

# SETTINGS
with tabs[4]:
    st.header("⚙️ Settings & Upload")
    uploaded_file = st.file_uploader("Upload XLSX", type="xlsx")
    threshold = st.slider("Coaching threshold",0,100,threshold)
    if uploaded_file:
        st.success("Processing…")
        df = pd.read_excel(uploaded_file)
        df.dropna(subset=['Agent','first_name','last_name','Advance'],inplace=True)
        df['Client'] = df.first_name.str.strip()+' '+df.last_name.str.strip()
        df['Paid Status'] = df.Advance.fillna(0).astype(float).apply(lambda x:'Paid' if x>0 else 'Not Paid')
        # compute totals & summary, ZIP and insert_report as before...
        st.download_button("Download ZIP", b"dummyzip", file_name="paystubs.zip")

# OVERVIEW
with tabs[0]:
    st.title("HCS Commission Dashboard")
    if uploaded_file:
        c1,c2,c3,c4=st.columns(4)
        c1.metric("Paid Deals","123"); c2.metric("Agent Payout","$456");
    else:
        if history_df.empty: st.info("Upload a statement…")
        else:
            latest = history_df.iloc[-1]
            c1,c2,c3,c4=st.columns(4)
            c1.metric("Total Paid Deals",f"{latest.total_deals:,}")

# LEADERBOARD
with tabs[1]:
    st.header("Agent Leaderboard")
    # show summary dataframe…

# HISTORY
with tabs[2]:
    st.header("Historical Reports")
    if not history_df.empty:
        sel = st.selectbox("Choose date:",[d.strftime('%Y-%m-%d') for d in history_df.upload_date])
        rec=history_df[history_df.upload_date.dt.strftime('%Y-%m-%d')==sel].iloc[0]
        cols=st.columns(4)
        cols[0].metric("Deals",f"{rec.total_deals:,}")

# LIVE COUNTS
with tabs[3]:
    st.header("Live Counts")
    df_api = fetch_today_leads()
    if not df_api.empty:
        df_api['date_sold']=pd.to_datetime(df_api['date_sold'],errors='coerce')
        today=date.today()
        d=df_api[df_api.date_sold.dt.date==today]
        st.metric("Today's Deals",len(d))

# CLIENTS
with tabs[5]:
    st.header("Clients Today")
    df_api = fetch_today_leads()
    if not df_api.empty:
        df_api['date_sold']=pd.to_datetime(df_api['date_sold'],errors='coerce')
        today=date.today()
        today_df=df_api[df_api.date_sold.dt.date==today]
        st.dataframe(today_df)









    






