# HCS Automated Commission System

This application provides a comprehensive dashboard for managing Health Connect Solutions (HCS) commission tracking, agent performance, and client management.

## Features

- **Agent Dashboard**: Displays individual agent performance, commission tiers, and earnings
- **Admin Dashboard**: Comprehensive overview of all agents, deals, and financial metrics
- **Commission Tracking**: Automated calculation of tiered commissions and bonuses
- **Vendor Management**: Track and calculate vendor payments and CPL/CPA metrics
- **Report Generation**: Create PDF paystubs and commission reports
- **Database Storage**: Persistent storage of all commission data, agents, and deals

## Database Structure

The application uses SQLite to store all data in a file named `hcs_database.db`. The database includes the following tables:

### Users Table
Stores admin user credentials and information:
- `username` (primary key)
- `password`
- `first_name`
- `last_name`
- `role`
- `created_at`
- `updated_at`

### Agents Table
Stores agent information from the CRM:
- `user_id` (primary key)
- `username`
- `first_name`
- `last_name`
- `role`
- `role_descriptions`
- `is_active`
- `created_at`
- `updated_at`

### Commission Cycles Table
Defines pay periods:
- `cycle_id` (primary key)
- `start_date`
- `end_date`
- `pay_date`

### Deals Table
Stores all policy/deal information:
- `policy_id` (primary key)
- `date_created`
- `date_converted`
- `date_sold`
- `date_posted`
- `carrier`
- `product`
- `duration`
- `premium`
- `policy_number`
- `lead_first_name`
- `lead_last_name`
- `lead_state`
- `lead_vendor_name`
- `agent_id` (foreign key to agents)
- `agent_name`
- `member_count`
- `paid_status`
- `advance_amount`
- `reason`

### Reports Table
Stores historical payroll reports:
- `upload_date` (primary key)
- `total_deals`
- `agent_payout`
- `owner_revenue`
- `owner_profit`

### Vendors Table
Stores vendor information and payment rates:
- `vendor_id` (primary key)
- `vendor_name`
- `vendor_code`
- `rate`
- `cpl`
- `is_active`
- `created_at`
- `updated_at`

### Tier Rates Table
Defines commission tier structure:
- `tier_id` (primary key)
- `min_deals`
- `rate`
- `bonus`
- `description`
- `effective_date`
- `is_active`

## Database Module Functions

The `database.py` file provides the following key functions:

### Initialization Functions
- `init_database()`: Creates all tables if they don't exist
- `import_commission_cycles(cycles_df)`: Imports commission cycles from DataFrame
- `import_users_from_csv(csv_path)`: Imports users from CSV file
- `import_vendors()`: Imports default vendors with rates
- `import_tier_rates()`: Imports default tier rates
- `import_agents_from_api(agents_df)`: Imports agents from API data

### Data Management Functions
- `save_deals_from_api(deals_df)`: Saves deals from API to database
- `update_deals_from_fmo(fmo_df, hs_df)`: Updates deals with FMO payment data and Health Sherpa member counts
- `save_report(upload_date, totals)`: Saves a new report to the database

### Query Functions
- `get_agent(username)`: Gets agent data by username
- `get_user(username)`: Gets user data by username
- `get_deals_by_agent(agent_id, date_from, date_to)`: Gets deals by agent ID and date range
- `get_all_deals_by_date(date_from, date_to)`: Gets all deals by date range
- `get_deals_by_vendor(vendor_code, date_from, date_to)`: Gets deals by vendor code and date range
- `get_current_commission_cycle()`: Gets the current commission cycle based on today's date
- `get_previous_commission_cycle()`: Gets the previous commission cycle
- `get_all_reports()`: Gets all reports in chronological order
- `get_vendors()`: Gets all active vendors
- `get_tier_rates()`: Gets all active tier rates
- `get_all_users()`: Gets all users
- `get_all_agents()`: Gets all agents
- `authenticate_user(username, password)`: Authenticates a user

## Setup and Usage

1. Place the `database.py` file in the same directory as `app.py`
2. If you have a `users.csv` file, place it in the working directory
3. Run the application with `streamlit run app.py`
4. The database will be automatically initialized on first run

## Data Import

The application can import data from:

- FMO statements (Excel files)
- Health Sherpa exports (CSV files)
- TLD CRM API (fetched automatically)

## Reports and Exports

The application can generate:

- Agent paystubs (PDF)
- Vendor payment reports (PDF)
- Commission summaries (CSV)
- CPL/CPA reports (CSV)

## Authentication

The application supports two types of users:

1. **Admin Users**: Loaded from the `users.csv` file or created in the database
2. **Agent Users**: Loaded from the TLD CRM API with a default password

## Additional Information

- The application automatically syncs agent data from the CRM API
- Commission cycles are predefined but can be modified
- Vendor rates can be customized in the database
- Historical reports are stored for trend analysis
