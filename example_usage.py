"""
Example usage of the database module for HCS Automated Commission System

This file demonstrates how to use the various functions in the database.py module
to interact with the HCS database.
"""

import pandas as pd
from datetime import datetime, timedelta
from database import *

def example_initialize_database():
    """Example of initializing the database and importing default data"""
    print("\n=== Initializing Database ===")
    
    # Initialize the database (creates tables if they don't exist)
    init_database()
    print("Database initialized")
    
    # Import default vendors
    import_vendors()
    print("Default vendors imported")
    
    # Import default tier rates
    import_tier_rates()
    print("Default tier rates imported")
    
    # Example of importing commission cycles
    cycles_df = pd.DataFrame([
        ("2025-01-01", "2025-01-15", "2025-01-22"),
        ("2025-01-16", "2025-01-31", "2025-02-07"),
    ], columns=["start", "end", "pay"])
    
    import_commission_cycles(cycles_df)
    print("Commission cycles imported")

def example_import_users():
    """Example of importing users from CSV"""
    print("\n=== Importing Users ===")
    
    # Create a sample users CSV
    users_df = pd.DataFrame([
        ["admin", "password123", "Admin", "User", "admin"],
        ["manager", "manager123", "Manager", "User", "manager"],
    ], columns=["username", "password", "first_name", "last_name", "role"])
    
    users_df.to_csv("sample_users.csv", index=False)
    print("Sample users CSV created")
    
    # Import users from CSV
    import_users_from_csv("sample_users.csv")
    print("Users imported")
    
    # Verify users were imported
    users = get_all_users()
    print(f"Users in database: {len(users)}")
    print(users)

def example_import_agents():
    """Example of importing agents from API data"""
    print("\n=== Importing Agents ===")
    
    # Create sample agent data (normally from API)
    agents_df = pd.DataFrame([
        ["A001", "agent1", "John", "Doe", "Agent", "Sales Agent"],
        ["A002", "agent2", "Jane", "Smith", "Agent", "Senior Agent"],
    ], columns=["user_id", "username", "first_name", "last_name", "role", "role_descriptions"])
    
    # Import agents
    import_agents_from_api(agents_df)
    print("Agents imported")
    
    # Verify agents were imported
    agents = get_all_agents()
    print(f"Agents in database: {len(agents)}")
    print(agents)

def example_import_deals():
    """Example of importing deals from API data"""
    print("\n=== Importing Deals ===")
    
    # Create sample deal data (normally from API)
    today = datetime.now().date()
    yesterday = (datetime.now() - timedelta(days=1)).date()
    
    deals_df = pd.DataFrame([
        ["P001", today, "Aetna", "Health", 150.0, "John", "Customer", "FL", "francalls", "A001", "John Doe"],
        ["P002", yesterday, "Cigna", "Dental", 85.0, "Alice", "Johnson", "TX", "acaking", "A002", "Jane Smith"],
    ], columns=["policy_id", "date_sold", "carrier", "product", "premium", 
                "lead_first_name", "lead_last_name", "lead_state", "lead_vendor_name", 
                "agent_id", "agent_name"])
    
    # Import deals
    save_deals_from_api(deals_df)
    print("Deals imported")
    
    # Create sample FMO payment data
    fmo_df = pd.DataFrame([
        ["John", "Customer", 150.0, ""],
        ["Alice", "Johnson", 0.0, "Incomplete paperwork"],
    ], columns=["first_name", "last_name", "Advance", "Reason"])
    
    # Create sample Health Sherpa data
    hs_df = pd.DataFrame([
        ["John", "Customer", "2"],
        ["Alice", "Johnson", "1"],
    ], columns=["first_name", "last_name", "applicant_count"])
    
    # Update deals with payment status and member counts
    update_deals_from_fmo(fmo_df, hs_df)
    print("Deals updated with payment status and member counts")

def example_query_data():
    """Example of querying data from the database"""
    print("\n=== Querying Data ===")
    
    # Get the current commission cycle
    current_cycle = get_current_commission_cycle()
    print("Current commission cycle:")
    print(current_cycle)
    
    # Get all deals from the last 30 days
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    deals = get_all_deals_by_date(start_date)
    print(f"\nDeals in the last 30 days: {len(deals)}")
    print(deals)
    
    # Get deals for a specific agent
    agent_deals = get_deals_by_agent("A001", start_date, datetime.now().strftime("%Y-%m-%d"))
    print(f"\nDeals for agent A001: {len(agent_deals)}")
    print(agent_deals)
    
    # Get deals for a specific vendor
    vendor_deals = get_deals_by_vendor("francalls", start_date)
    print(f"\nDeals for vendor 'francalls': {len(vendor_deals)}")
    print(vendor_deals)
    
    # Get all vendors
    vendors = get_vendors()
    print(f"\nActive vendors: {len(vendors)}")
    print(vendors)
    
    # Get tier rates
    tiers = get_tier_rates()
    print(f"\nTier rates: {len(tiers)}")
    print(tiers)

def example_save_report():
    """Example of saving a report"""
    print("\n=== Saving Report ===")
    
    # Create sample report data
    totals = {
        "deals": 45,
        "agent": 12350.0,
        "owner_rev": 6750.0,
        "owner_prof": 3800.0
    }
    
    # Save the report
    upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_report(upload_date, totals)
    print("Report saved")
    
    # Get all reports
    reports = get_all_reports()
    print(f"Total reports: {len(reports)}")
    print(reports)

def example_authentication():
    """Example of authenticating users"""
    print("\n=== Authentication ===")
    
    # Authenticate admin user
    auth_admin, user_type_admin, user_data_admin = authenticate_user("admin", "password123")
    print(f"Admin auth success: {auth_admin}")
    print(f"User type: {user_type_admin}")
    print(f"User data: {user_data_admin}")
    
    # Authenticate agent user
    auth_agent, user_type_agent, user_data_agent = authenticate_user("agent1", "password")
    print(f"\nAgent auth success: {auth_agent}")
    print(f"User type: {user_type_agent}")
    print(f"User data: {user_data_agent}")
    
    # Failed authentication
    auth_fail, user_type_fail, user_data_fail = authenticate_user("wrong", "wrong")
    print(f"\nFailed auth: {auth_fail}")
    print(f"User type: {user_type_fail}")
    print(f"User data: {user_data_fail}")

if __name__ == "__main__":
    print("HCS Database Examples")
    print("=====================")
    
    example_initialize_database()
    example_import_users()
    example_import_agents()
    example_import_deals()
    example_query_data()
    example_save_report()
    example_authentication()
    
    print("\nExamples completed.")
