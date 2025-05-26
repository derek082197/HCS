import sqlite3
import pandas as pd
import os
from datetime import datetime

# Database file path
DB_PATH = "hcs_database.db"

def get_connection():
    """Get a connection to the database"""
    return sqlite3.connect(DB_PATH)

def init_database():
    """Initialize the database with all required tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Create users table (for admin users)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create agents table (from CRM)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS agents (
        user_id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        role TEXT,
        role_descriptions TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create commission_cycles table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS commission_cycles (
        cycle_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        pay_date DATE NOT NULL
    )
    ''')
    
    # Create deals table (for storing policy data)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS deals (
        policy_id TEXT PRIMARY KEY,
        date_created DATE,
        date_converted DATE,
        date_sold DATE,
        date_posted DATE,
        carrier TEXT,
        product TEXT,
        duration TEXT,
        premium REAL,
        policy_number TEXT,
        lead_first_name TEXT,
        lead_last_name TEXT,
        lead_state TEXT,
        lead_vendor_name TEXT,
        agent_id TEXT,
        agent_name TEXT,
        member_count INTEGER DEFAULT 1,
        paid_status TEXT DEFAULT 'Pending',
        advance_amount REAL DEFAULT 0,
        reason TEXT,
        FOREIGN KEY (agent_id) REFERENCES agents(user_id)
    )
    ''')
    
    # Create reports table (similar to the existing one)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reports (
        upload_date TEXT PRIMARY KEY,
        total_deals INTEGER,
        agent_payout REAL,
        owner_revenue REAL,
        owner_profit REAL
    )
    ''')
    
    # Create vendors table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS vendors (
        vendor_id TEXT PRIMARY KEY,
        vendor_name TEXT NOT NULL,
        vendor_code TEXT NOT NULL,
        rate REAL DEFAULT 0,
        cpl REAL DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create tier_rates table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tier_rates (
        tier_id INTEGER PRIMARY KEY AUTOINCREMENT,
        min_deals INTEGER NOT NULL,
        rate REAL NOT NULL,
        bonus REAL DEFAULT 0,
        description TEXT,
        effective_date DATE DEFAULT CURRENT_TIMESTAMP,
        is_active INTEGER DEFAULT 1
    )
    ''')
    
    conn.commit()
    conn.close()

def import_commission_cycles(cycles_df):
    """Import commission cycles from DataFrame"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # First check if table is empty
    cursor.execute("SELECT COUNT(*) FROM commission_cycles")
    count = cursor.fetchone()[0]
    
    # Only import if table is empty
    if count == 0:
        for _, row in cycles_df.iterrows():
            cursor.execute('''
            INSERT INTO commission_cycles (start_date, end_date, pay_date)
            VALUES (?, ?, ?)
            ''', (row['start'], row['end'], row['pay']))
    
    conn.commit()
    conn.close()

def import_users_from_csv(csv_path):
    """Import users from CSV file"""
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return
        
    df_users = pd.read_csv(csv_path, dtype=str).dropna()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if table has data
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    
    # Only import if table is empty
    if count == 0:
        for _, row in df_users.iterrows():
            cursor.execute('''
            INSERT INTO users (username, password, first_name, last_name, role)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                row['username'].strip(), 
                row['password'], 
                row['first_name'], 
                row['last_name'], 
                row['role']
            ))
    
    conn.commit()
    conn.close()

def import_vendors():
    """Import default vendors with rates"""
    vendors = [
        ("francalls", "Fran Calls", "francalls", 75, 25),
        ("hcsmedia", "HCS Media", "hcsmedia", 75, 0),
        ("buffercall", "Aetna", "buffercall", 80, 0),
        ("acaking", "ACA KING", "acaking", 75, 35),
        ("raycalls", "RAY CALLS", "raycalls", 75, 0),
        ("joshaca", "JOSH ACA", "joshaca", 0, 30),
        # Add more vendors as needed
    ]
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if table has data
    cursor.execute("SELECT COUNT(*) FROM vendors")
    count = cursor.fetchone()[0]
    
    # Only import if table is empty
    if count == 0:
        for vendor in vendors:
            cursor.execute('''
            INSERT INTO vendors (vendor_id, vendor_name, vendor_code, rate, cpl)
            VALUES (?, ?, ?, ?, ?)
            ''', vendor)
    
    conn.commit()
    conn.close()

def import_tier_rates():
    """Import default tier rates"""
    tiers = [
        (0, 15, 0, "Starter Tier"),
        (70, 15, 1200, "Bonus Tier"),
        (120, 17.5, 1200, "Rising Tier"),
        (150, 22.5, 1200, "Pro Tier"),
        (200, 25, 1200, "Top Tier")
    ]
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if table has data
    cursor.execute("SELECT COUNT(*) FROM tier_rates")
    count = cursor.fetchone()[0]
    
    # Only import if table is empty
    if count == 0:
        for tier in tiers:
            cursor.execute('''
            INSERT INTO tier_rates (min_deals, rate, bonus, description)
            VALUES (?, ?, ?, ?)
            ''', tier)
    
    conn.commit()
    conn.close()

def import_agents_from_api(agents_df):
    """Import agents from API data"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Use INSERT OR REPLACE to update existing entries
    for _, row in agents_df.iterrows():
        cursor.execute('''
        INSERT OR REPLACE INTO agents 
        (user_id, username, first_name, last_name, role, role_descriptions, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            row['user_id'],
            row['username'],
            row['first_name'],
            row['last_name'],
            row['role'],
            row.get('role_descriptions', ''),
        ))
    
    conn.commit()
    conn.close()

def save_deals_from_api(deals_df):
    """Save deals from API to database"""
    if deals_df.empty:
        return
        
    conn = get_connection()
    cursor = conn.cursor()
    
    for _, row in deals_df.iterrows():
        cursor.execute('''
        INSERT OR REPLACE INTO deals 
        (policy_id, date_sold, carrier, product, premium, lead_first_name, lead_last_name, 
         lead_state, lead_vendor_name, agent_id, agent_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.get('policy_id', ''),
            row.get('date_sold', None),
            row.get('carrier', ''),
            row.get('product', ''),
            row.get('premium', 0),
            row.get('lead_first_name', ''),
            row.get('lead_last_name', ''),
            row.get('lead_state', ''),
            row.get('lead_vendor_name', ''),
            row.get('agent_id', ''),
            row.get('agent_name', '')
        ))
    
    conn.commit()
    conn.close()

def update_deals_from_fmo(fmo_df, hs_df=None):
    """Update deals with FMO payment data and Health Sherpa member counts"""
    if fmo_df.empty:
        return
        
    conn = get_connection()
    cursor = conn.cursor()
    
    # Create lookup for Health Sherpa member counts
    member_lookup = {}
    if hs_df is not None and not hs_df.empty:
        hs_df['first_name_norm'] = hs_df['first_name'].astype(str).str.strip().str.lower()
        hs_df['last_name_norm'] = hs_df['last_name'].astype(str).str.strip().str.lower()
        hs_df['member_count'] = pd.to_numeric(hs_df['applicant_count'], errors='coerce').fillna(1).astype(int)
        member_lookup = hs_df.set_index(['first_name_norm','last_name_norm'])['member_count'].to_dict()
    
    # Process FMO data
    fmo_df['first_name_norm'] = fmo_df['first_name'].astype(str).str.strip().str.lower()
    fmo_df['last_name_norm'] = fmo_df['last_name'].astype(str).str.strip().str.lower()
    advance_col = next((c for c in fmo_df.columns if "advance" in c.lower()), None)
    reason_col = next((c for c in fmo_df.columns if "reason" in c.lower()), None)
    
    if advance_col:
        fmo_df[advance_col] = pd.to_numeric(fmo_df[advance_col], errors='coerce').fillna(0)
        
        # Get all deals in the database
        cursor.execute('''
        SELECT policy_id, lead_first_name, lead_last_name FROM deals
        ''')
        deals = cursor.fetchall()
        
        # Normalize deal names for matching
        deal_map = {}
        for policy_id, fname, lname in deals:
            if fname and lname:
                key = (fname.strip().lower(), lname.strip().lower())
                deal_map[key] = policy_id
        
        # Update deals based on FMO data
        for _, row in fmo_df.iterrows():
            key = (row['first_name_norm'], row['last_name_norm'])
            policy_id = deal_map.get(key)
            
            if policy_id:
                # Update deal status
                advance = row[advance_col]
                paid_status = "Paid" if advance > 0 else "Not Paid"
                reason = row.get(reason_col, "") if reason_col else ""
                
                # Get member count from Health Sherpa
                member_count = member_lookup.get(key, 1)
                
                cursor.execute('''
                UPDATE deals SET 
                    paid_status = ?,
                    advance_amount = ?,
                    reason = ?,
                    member_count = ?
                WHERE policy_id = ?
                ''', (paid_status, advance, reason, member_count, policy_id))
    
    conn.commit()
    conn.close()

def save_report(upload_date, totals):
    """Save a new report to the database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT OR REPLACE INTO reports
    (upload_date, total_deals, agent_payout, owner_revenue, owner_profit)
    VALUES (?, ?, ?, ?, ?)
    ''', (upload_date, totals["deals"], totals["agent"], totals["owner_rev"], totals["owner_prof"]))
    
    conn.commit()
    conn.close()

def get_agent(username):
    """Get agent data by username"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM agents WHERE username = ?
    ''', (username,))
    
    agent = cursor.fetchone()
    conn.close()
    
    if agent:
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, agent))
    return None

def get_user(username):
    """Get user data by username"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM users WHERE username = ?
    ''', (username,))
    
    user = cursor.fetchone()
    conn.close()
    
    if user:
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, user))
    return None

def get_deals_by_agent(agent_id, date_from, date_to):
    """Get deals by agent ID and date range"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM deals 
    WHERE agent_id = ? 
    AND date_sold BETWEEN ? AND ?
    ORDER BY date_sold DESC
    ''', (agent_id, date_from, date_to))
    
    deals = cursor.fetchall()
    
    if deals:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for deal in deals:
            result.append(dict(zip(columns, deal)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_all_deals_by_date(date_from, date_to=None):
    """Get all deals by date range"""
    conn = get_connection()
    cursor = conn.cursor()
    
    if date_to:
        cursor.execute('''
        SELECT * FROM deals 
        WHERE date_sold BETWEEN ? AND ?
        ORDER BY date_sold DESC
        ''', (date_from, date_to))
    else:
        cursor.execute('''
        SELECT * FROM deals 
        WHERE date_sold >= ?
        ORDER BY date_sold DESC
        ''', (date_from,))
    
    deals = cursor.fetchall()
    
    if deals:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for deal in deals:
            result.append(dict(zip(columns, deal)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_deals_by_vendor(vendor_code, date_from=None, date_to=None):
    """Get deals by vendor code and optional date range"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = "SELECT * FROM deals WHERE lead_vendor_name = ?"
    params = [vendor_code]
    
    if date_from:
        query += " AND date_sold >= ?"
        params.append(date_from)
        
    if date_to:
        query += " AND date_sold <= ?"
        params.append(date_to)
    
    cursor.execute(query, params)
    deals = cursor.fetchall()
    
    if deals:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for deal in deals:
            result.append(dict(zip(columns, deal)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_current_commission_cycle():
    """Get the current commission cycle based on today's date"""
    today = datetime.now().date()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM commission_cycles
    WHERE start_date <= ? AND end_date >= ?
    ''', (today, today))
    
    cycle = cursor.fetchone()
    
    if cycle:
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, cycle))
    
    conn.close()
    return None

def get_previous_commission_cycle():
    """Get the previous commission cycle"""
    current = get_current_commission_cycle()
    
    if not current:
        return None
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM commission_cycles
    WHERE end_date < ?
    ORDER BY end_date DESC
    LIMIT 1
    ''', (current['start_date'],))
    
    cycle = cursor.fetchone()
    
    if cycle:
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, cycle))
    
    conn.close()
    return None

def get_all_reports():
    """Get all reports in chronological order"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM reports ORDER BY upload_date
    ''')
    
    reports = cursor.fetchall()
    
    if reports:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for report in reports:
            result.append(dict(zip(columns, report)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_vendors():
    """Get all active vendors"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM vendors WHERE is_active = 1
    ''')
    
    vendors = cursor.fetchall()
    
    if vendors:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for vendor in vendors:
            result.append(dict(zip(columns, vendor)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_tier_rates():
    """Get all active tier rates"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM tier_rates WHERE is_active = 1
    ORDER BY min_deals
    ''')
    
    tiers = cursor.fetchall()
    
    if tiers:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for tier in tiers:
            result.append(dict(zip(columns, tier)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_all_users():
    """Get all users"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT username, first_name, last_name, role FROM users
    ''')
    
    users = cursor.fetchall()
    
    if users:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for user in users:
            result.append(dict(zip(columns, user)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def get_all_agents():
    """Get all agents"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM agents WHERE is_active = 1
    ''')
    
    agents = cursor.fetchall()
    
    if agents:
        columns = [desc[0] for desc in cursor.description]
        result = []
        for agent in agents:
            result.append(dict(zip(columns, agent)))
        return pd.DataFrame(result)
    
    conn.close()
    return pd.DataFrame()

def authenticate_user(username, password):
    """Authenticate a user"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check admin users first
    cursor.execute('''
    SELECT username, first_name, last_name, role FROM users
    WHERE username = ? AND password = ?
    ''', (username, password))
    
    user = cursor.fetchone()
    
    if user:
        columns = [desc[0] for desc in cursor.description]
        return True, "Admin", dict(zip(columns, user))
    
    # Then check agents
    cursor.execute('''
    SELECT username, first_name, last_name, role_descriptions, user_id FROM agents
    WHERE username = ? AND is_active = 1
    ''', (username,))
    
    agent = cursor.fetchone()
    
    conn.close()
    
    if agent:
        # For agents, we're using a default password (as in the original code)
        if password == 'password':
            columns = ['username', 'first_name', 'last_name', 'role', 'user_id']
            return True, "Agent", dict(zip(columns, agent))
    
    return False, "", None

# Initialize database on import
if not os.path.exists(DB_PATH):
    init_database()
    import_tier_rates()
    import_vendors()
