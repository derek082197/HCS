# HCS Automated Commission System - Installation Guide

This guide will help you set up the HCS Automated Commission System with the new database implementation.

## Prerequisites

- Python 3.7 or higher
- Required Python packages:
  - streamlit
  - pandas
  - sqlite3
  - fpdf
  - requests
  - streamlit_extras (optional, for auto-refresh)

## Installation Steps

1. **Clone or download the repository**

   Place all files in a directory of your choice.

2. **Install required packages**

   ```
   pip install streamlit pandas fpdf requests
   pip install streamlit-extras  # Optional, for auto-refresh
   ```

3. **Set up users (optional)**

   Create a `users.csv` file in the same directory with the following columns:
   - username
   - password
   - first_name
   - last_name
   - role

   Example:
   ```
   username,password,first_name,last_name,role
   admin,admin123,Admin,User,admin
   manager,manager123,Manager,User,manager
   ```

4. **Run the application**

   ```
   streamlit run app.py
   ```

   The database will be automatically initialized on the first run.

## Testing the Database

You can test the database functionality by running the test script:

```
python test_database.py
```

This will run a series of unit tests to verify that all database functions are working correctly.

## Exploring Database Examples

To see examples of how to use the database module, you can run:

```
python example_usage.py
```

This will demonstrate various database operations like importing data, querying records, and generating reports.

## Database File

The system uses SQLite to store all data in a file named `hcs_database.db`. This file will be created automatically when you first run the application.

## Default Data

The system comes pre-configured with:

- Default commission cycles
- Default vendor rates
- Default tier rates

These can be modified in the database.py file if needed.

## Importing Data

The application can import data from:

1. **TLD CRM API** - Agents and deals are automatically fetched from the API
2. **FMO statements** - Upload Excel files via the application interface
3. **Health Sherpa exports** - Upload CSV files via the application interface
4. **Users CSV** - Place a users.csv file in the working directory

## Backup Recommendations

It's recommended to regularly back up the `hcs_database.db` file to prevent data loss. The file can be found in the application directory.

## Troubleshooting

- If you encounter database errors, try removing the `hcs_database.db` file and restarting the application. This will recreate the database from scratch.
- If authentication issues occur, check the users.csv file format or try the default agent password "password".
- For API connection issues, verify the API credentials in app.py.

## Support

For additional support, please refer to the README.md file or contact the system administrator.
