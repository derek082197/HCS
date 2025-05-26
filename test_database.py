"""
Unit tests for the database module of HCS Automated Commission System

Run these tests to verify that the database functionality works correctly.
This file also serves as additional documentation on how to use the database module.
"""

import unittest
import os
import pandas as pd
from datetime import datetime, timedelta
from database import *

class TestDatabaseFunctions(unittest.TestCase):
    """Test case for database functions"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment - create a test database"""
        # Use a test database file
        global DB_PATH
        cls.original_db_path = DB_PATH
        DB_PATH = "test_hcs_database.db"
        
        # Remove test database if it exists
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            
        # Initialize database
        init_database()
        
    @classmethod
    def tearDownClass(cls):
        """Clean up after tests"""
        # Remove test database
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            
        # Restore original database path
        global DB_PATH
        DB_PATH = cls.original_db_path
    
    def test_01_init_database(self):
        """Test database initialization"""
        # Should not raise an exception
        init_database()
        
        # Verify the database file exists
        self.assertTrue(os.path.exists(DB_PATH))
        
        # Get a connection and check if tables exist
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Check if all expected tables exist
        expected_tables = [
            'users', 'agents', 'commission_cycles', 'deals', 
            'reports', 'vendors', 'tier_rates'
        ]
        for table in expected_tables:
            self.assertIn(table, tables)
            
        conn.close()
    
    def test_02_import_commission_cycles(self):
        """Test importing commission cycles"""
        # Create test data
        cycles_df = pd.DataFrame([
            ("2025-01-01", "2025-01-15", "2025-01-22"),
            ("2025-01-16", "2025-01-31", "2025-02-07"),
        ], columns=["start", "end", "pay"])
        
        # Import commission cycles
        import_commission_cycles(cycles_df)
        
        # Get a connection and check if data was imported
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM commission_cycles")
        count = cursor.fetchone()[0]
        conn.close()
        
        # Should have imported 2 cycles
        self.assertEqual(count, 2)
    
    def test_03_import_users(self):
        """Test importing users from CSV"""
        # Create a temporary CSV file
        users_df = pd.DataFrame([
            ["admin", "password123", "Admin", "User", "admin"],
            ["manager", "manager123", "Manager", "User", "manager"],
        ], columns=["username", "password", "first_name", "last_name", "role"])
        
        temp_csv = "temp_users.csv"
        users_df.to_csv(temp_csv, index=False)
        
        # Import users
        import_users_from_csv(temp_csv)
        
        # Clean up
        os.remove(temp_csv)
        
        # Verify users were imported
        users = get_all_users()
        self.assertEqual(len(users), 2)
        
        # Check specific user
        user = get_user("admin")
        self.assertIsNotNone(user)
        self.assertEqual(user["username"], "admin")
        self.assertEqual(user["password"], "password123")
        self.assertEqual(user["first_name"], "Admin")
        self.assertEqual(user["role"], "admin")
    
    def test_04_import_vendors(self):
        """Test importing default vendors"""
        # Import vendors
        import_vendors()
        
        # Get vendors
        vendors = get_vendors()
        
        # Should have vendors
        self.assertTrue(len(vendors) > 0)
        
        # Check specific vendor
        vendor_ids = vendors["vendor_id"].tolist()
        self.assertIn("francalls", vendor_ids)
        self.assertIn("acaking", vendor_ids)
    
    def test_05_import_tier_rates(self):
        """Test importing default tier rates"""
        # Import tier rates
        import_tier_rates()
        
        # Get tier rates
        tiers = get_tier_rates()
        
        # Should have tiers
        self.assertTrue(len(tiers) > 0)
        
        # Check specific tier
        # First tier should have min_deals = 0
        self.assertEqual(tiers.iloc[0]["min_deals"], 0)
        
        # Last tier should have min_deals = 200
        self.assertEqual(tiers.iloc[-1]["min_deals"], 200)
    
    def test_06_import_agents(self):
        """Test importing agents"""
        # Create test data
        agents_df = pd.DataFrame([
            ["A001", "agent1", "John", "Doe", "Agent", "Sales Agent"],
            ["A002", "agent2", "Jane", "Smith", "Agent", "Senior Agent"],
        ], columns=["user_id", "username", "first_name", "last_name", "role", "role_descriptions"])
        
        # Import agents
        import_agents_from_api(agents_df)
        
        # Get agents
        agents = get_all_agents()
        
        # Should have agents
        self.assertEqual(len(agents), 2)
        
        # Check specific agent
        agent = get_agent("agent1")
        self.assertIsNotNone(agent)
        self.assertEqual(agent["user_id"], "A001")
        self.assertEqual(agent["first_name"], "John")
        self.assertEqual(agent["last_name"], "Doe")
    
    def test_07_save_deals(self):
        """Test saving deals from API"""
        # Create test data
        today = datetime.now().date()
        yesterday = (datetime.now() - timedelta(days=1)).date()
        
        deals_df = pd.DataFrame([
            ["P001", today, "Aetna", "Health", 150.0, "John", "Customer", "FL", "francalls", "A001", "John Doe"],
            ["P002", yesterday, "Cigna", "Dental", 85.0, "Alice", "Johnson", "TX", "acaking", "A002", "Jane Smith"],
        ], columns=["policy_id", "date_sold", "carrier", "product", "premium", 
                    "lead_first_name", "lead_last_name", "lead_state", "lead_vendor_name", 
                    "agent_id", "agent_name"])
        
        # Save deals
        save_deals_from_api(deals_df)
        
        # Get deals
        start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        deals = get_all_deals_by_date(start_date, end_date)
        
        # Should have deals
        self.assertEqual(len(deals), 2)
        
        # Check specific deal
        self.assertIn("P001", deals["policy_id"].tolist())
        self.assertIn("P002", deals["policy_id"].tolist())
    
    def test_08_update_deals(self):
        """Test updating deals with FMO data"""
        # Create FMO test data
        fmo_df = pd.DataFrame([
            ["John", "Customer", 150.0, ""],
            ["Alice", "Johnson", 0.0, "Incomplete paperwork"],
        ], columns=["first_name", "last_name", "Advance", "Reason"])
        
        # Create Health Sherpa test data
        hs_df = pd.DataFrame([
            ["John", "Customer", "2"],
            ["Alice", "Johnson", "1"],
        ], columns=["first_name", "last_name", "applicant_count"])
        
        # Update deals
        update_deals_from_fmo(fmo_df, hs_df)
        
        # Get deals
        start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        deals = get_all_deals_by_date(start_date, end_date)
        
        # Check deal updates
        john_deal = deals[deals["lead_first_name"] == "John"].iloc[0]
        self.assertEqual(john_deal["paid_status"], "Paid")
        self.assertEqual(john_deal["advance_amount"], 150.0)
        self.assertEqual(john_deal["member_count"], 2)
        
        alice_deal = deals[deals["lead_first_name"] == "Alice"].iloc[0]
        self.assertEqual(alice_deal["paid_status"], "Not Paid")
        self.assertEqual(alice_deal["advance_amount"], 0.0)
        self.assertEqual(alice_deal["reason"], "Incomplete paperwork")
        self.assertEqual(alice_deal["member_count"], 1)
    
    def test_09_save_report(self):
        """Test saving a report"""
        # Create test data
        totals = {
            "deals": 45,
            "agent": 12350.0,
            "owner_rev": 6750.0,
            "owner_prof": 3800.0
        }
        
        # Save report
        upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_report(upload_date, totals)
        
        # Get reports
        reports = get_all_reports()
        
        # Should have reports
        self.assertEqual(len(reports), 1)
        
        # Check report data
        report = reports.iloc[0]
        self.assertEqual(report["total_deals"], 45)
        self.assertEqual(report["agent_payout"], 12350.0)
        self.assertEqual(report["owner_revenue"], 6750.0)
        self.assertEqual(report["owner_profit"], 3800.0)
    
    def test_10_authenticate_user(self):
        """Test user authentication"""
        # Test admin authentication
        auth_admin, user_type_admin, user_data_admin = authenticate_user("admin", "password123")
        self.assertTrue(auth_admin)
        self.assertEqual(user_type_admin, "Admin")
        self.assertEqual(user_data_admin["username"], "admin")
        
        # Test agent authentication
        auth_agent, user_type_agent, user_data_agent = authenticate_user("agent1", "password")
        self.assertTrue(auth_agent)
        self.assertEqual(user_type_agent, "Agent")
        self.assertEqual(user_data_agent["username"], "agent1")
        
        # Test failed authentication
        auth_fail, user_type_fail, user_data_fail = authenticate_user("wrong", "wrong")
        self.assertFalse(auth_fail)
        self.assertEqual(user_type_fail, "")
        self.assertIsNone(user_data_fail)
    
    def test_11_get_commission_cycles(self):
        """Test getting commission cycles"""
        # Get current cycle
        current_cycle = get_current_commission_cycle()
        
        # May be None if today's date doesn't fall within a cycle
        if current_cycle:
            self.assertIsInstance(current_cycle, dict)
            self.assertIn("start_date", current_cycle)
            self.assertIn("end_date", current_cycle)
            self.assertIn("pay_date", current_cycle)
        
        # Get previous cycle
        prev_cycle = get_previous_commission_cycle()
        
        # May be None if no previous cycle
        if prev_cycle:
            self.assertIsInstance(prev_cycle, dict)
            self.assertIn("start_date", prev_cycle)
            self.assertIn("end_date", prev_cycle)
            self.assertIn("pay_date", prev_cycle)
    
    def test_12_get_deals_by_agent(self):
        """Test getting deals by agent"""
        # Get deals for agent A001
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        deals = get_deals_by_agent("A001", start_date, end_date)
        
        # Should have deals
        self.assertEqual(len(deals), 1)
        
        # Check deal
        deal = deals.iloc[0]
        self.assertEqual(deal["agent_id"], "A001")
        self.assertEqual(deal["lead_first_name"], "John")
        self.assertEqual(deal["lead_last_name"], "Customer")
    
    def test_13_get_deals_by_vendor(self):
        """Test getting deals by vendor"""
        # Get deals for vendor francalls
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        deals = get_deals_by_vendor("francalls", start_date)
        
        # Should have deals
        self.assertEqual(len(deals), 1)
        
        # Check deal
        deal = deals.iloc[0]
        self.assertEqual(deal["lead_vendor_name"], "francalls")
        self.assertEqual(deal["lead_first_name"], "John")
        self.assertEqual(deal["lead_last_name"], "Customer")

if __name__ == '__main__':
    unittest.main()
