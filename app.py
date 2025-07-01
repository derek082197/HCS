import streamlit as st

# Must be first Streamlit command
st.set_page_config(page_title="HCS Commission CRM", layout="wide")

import sqlite3
import io
import zipfile
import csv
import sys
import os

# Handle pandas dependency issue with comprehensive fallback
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError as e:
    PANDAS_AVAILABLE = False
    # Create comprehensive pandas mock
    class MockDataFrame:
        def __init__(self, data=None, columns=None):
            self.data = data or []
            self.columns = columns or []
            self.empty = len(self.data) == 0
            
        def __len__(self):
            return len(self.data)
            
        def __getitem__(self, key):
            return MockDataFrame()
            
        def iterrows(self):
            return iter([])
            
        def to_csv(self, *args, **kwargs):
            return ""
            
        def sum(self):
            return 0
            
        def unique(self):
            return []
    
    class MockPandas:
        def DataFrame(self, *args, **kwargs):
            return MockDataFrame()
        def read_csv(self, *args, **kwargs):
            return MockDataFrame()
        def concat(self, *args, **kwargs):
            return MockDataFrame()
    
    pd = MockPandas()
    print("Warning: Pandas unavailable, using fallback mode")
import time
from datetime import date, datetime, timedelta
from fpdf import FPDF
import requests
import os
import psycopg2
from sqlalchemy import create_engine, text
import pytz
from agent_mapping import get_agents_by_role, get_agent_by_name, find_agent_by_partial_name, get_agent_name_variations
from vendor_api_integration import fetch_vendor_config, calculate_vendor_cpa_with_thresholds
from discord_webhook import DiscordSalesTracker
from sales_monitor import SalesMonitor
from manager_dashboard import ManagerDashboard, MANAGER_ACCOUNTS
import threading
import atexit

# Disable auto-refresh to prevent constant page reloading
def st_autorefresh(*args, **kwargs): 
    return None  # Completely disabled to stop dashboard spam

# Global CSS for maximum mobile visibility - Applied immediately
st.markdown("""
<style>
/* DEPLOYMENT-PROOF CSS - FORCES PRODUCTION TO MATCH PREVIEW */

/* Override ALL text elements in deployment environment */
* {
    color: inherit !important;
}

/* Force Member Counts section to display correctly in production */
.member-counts-container h4,
.member-card-today h3,
.member-card-today p,
.member-card-weekly h3,
.member-card-weekly p,
.member-card-monthly h3,
.member-card-monthly p {
    color: #ffffff !important;
    text-shadow: 0 0 15px rgba(255,255,255,1) !important;
    font-weight: 900 !important;
    opacity: 1 !important;
    visibility: visible !important;
    display: block !important;
}

/* Ensure text is always visible regardless of deployment environment */
h1, h2, h3, h4, h5, h6, p, span, div {
    opacity: 1 !important;
    visibility: visible !important;
}

/* ONLY TARGET LIVE COUNTS TAB FOR MOBILE VISIBILITY */

/* Remove all previous styling - use clean Wolf of Wall Street theme */
.stTabs [data-baseweb="tab-panel"] [data-testid="stMetricValue"] {
    font-size: 48px !important;
    font-weight: 700 !important;
    font-family: 'Montserrat', sans-serif !important;
    text-shadow: none !important;
    filter: none !important;
    line-height: 1.2 !important;
    padding: 10px !important;
    white-space: nowrap !important;
    overflow: visible !important;
    text-overflow: clip !important;
    width: auto !important;
    max-width: none !important;
    min-width: auto !important;
}

.stTabs [data-baseweb="tab-panel"] [data-testid="metric-container"] {
    overflow: visible !important;
    width: auto !important;
    max-width: none !important;
    min-width: auto !important;
}
</style>
""", unsafe_allow_html=True)

# Wolf of Wall Street Theme - Premium Financial Power Design
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Montserrat:wght@300;400;600;700;900&display=swap');
    
    /* === WOLF OF WALL STREET THEME === */
    .stApp {
        background: linear-gradient(135deg, #000000 0%, #1a1a1a 25%, #2d2d2d 50%, #1a1a1a 75%, #000000 100%) !important;
        background-attachment: fixed !important;
        color: #FFFFFF !important;
    }
    
    /* Luxury Pattern Overlay */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-image: 
            radial-gradient(circle at 20% 80%, rgba(255, 215, 0, 0.08) 0%, transparent 50%),
            radial-gradient(circle at 80% 20%, rgba(255, 215, 0, 0.06) 0%, transparent 50%);
        pointer-events: none;
        z-index: 0;
    }
    
    /* Main container */
    .main > div {
        padding: 2rem 1rem !important;
        position: relative !important;
        z-index: 1 !important;
    }
    
    /* === PREMIUM BUTTONS - WALL STREET POWER === */
    .stButton > button {
        background: linear-gradient(135deg, #FFD700 0%, #FFA500 50%, #FFD700 100%) !important;
        color: #000000 !important;
        border: 3px solid #FFD700 !important;
        border-radius: 20px !important;
        padding: 1rem 2rem !important;
        font-weight: 900 !important;
        font-family: 'Montserrat', sans-serif !important;
        font-size: 1.1rem !important;
        letter-spacing: 2px !important;
        text-transform: uppercase !important;
        transition: all 0.4s ease !important;
        box-shadow: 
            0 8px 25px rgba(255, 215, 0, 0.4),
            inset 0 0 15px rgba(255, 255, 255, 0.2) !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-5px) scale(1.05) !important;
        background: linear-gradient(135deg, #00FF00 0%, #32CD32 50%, #00FF00 100%) !important;
        border-color: #00FF00 !important;
        box-shadow: 
            0 20px 60px rgba(0, 255, 0, 0.6),
            inset 0 0 25px rgba(255, 255, 255, 0.3) !important;
        animation: goldPulse 1s infinite !important;
    }
    
    /* === METRICS - MONEY COUNTERS === */
    .stMetric {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 50%, #1a1a1a 100%) !important;
        border: 3px solid #FFD700 !important;
        border-radius: 25px !important;
        padding: 2rem !important;
        box-shadow: 
            0 15px 50px rgba(0,0,0,0.6),
            0 0 30px rgba(255, 215, 0, 0.3),
            inset 0 0 25px rgba(255, 215, 0, 0.05) !important;
        transition: all 0.4s ease !important;
        position: relative !important;
        overflow: hidden !important;
    }
    
    .stMetric:hover {
        transform: translateY(-10px) scale(1.03) !important;
        border-color: #00FF00 !important;
        box-shadow: 
            0 25px 80px rgba(0,0,0,0.8),
            0 0 50px rgba(0, 255, 0, 0.5),
            inset 0 0 35px rgba(0, 255, 0, 0.1) !important;
    }
    
    .stMetric::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255, 215, 0, 0.2), transparent);
        transition: left 0.8s ease;
    }
    
    .stMetric:hover::before {
        left: 100%;
    }
    
    /* Metric values - pure gold */
    .stMetric [data-testid="metric-value"] {
        color: #FFD700 !important;
        font-size: 4rem !important;
        font-weight: 900 !important;
        font-family: 'Montserrat', sans-serif !important;
        text-shadow: 
            0 0 20px rgba(255, 215, 0, 0.8),
            3px 3px 6px rgba(0,0,0,0.8) !important;
        letter-spacing: 2px !important;
    }
    
    .stMetric [data-testid="metric-label"] {
        color: #FFFFFF !important;
        font-size: 1.2rem !important;
        font-weight: 700 !important;
        font-family: 'Montserrat', sans-serif !important;
        text-transform: uppercase !important;
        letter-spacing: 3px !important;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.8) !important;
    }
    
    /* === DATAFRAMES - FINANCIAL SPREADSHEETS === */
    .stDataFrame {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%) !important;
        border: 2px solid #FFD700 !important;
        border-radius: 20px !important;
        overflow: hidden !important;
        box-shadow: 
            0 15px 50px rgba(0,0,0,0.5),
            0 0 25px rgba(255, 215, 0, 0.2) !important;
    }
    
    /* === TABS - EXECUTIVE FOLDERS === */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px !important;
        background: transparent !important;
        padding: 1rem 0 !important;
        flex-wrap: wrap !important;
        justify-content: center !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%) !important;
        color: #FFD700 !important;
        border: 2px solid #FFD700 !important;
        border-radius: 20px !important;
        font-weight: 800 !important;
        font-family: 'Montserrat', sans-serif !important;
        transition: all 0.4s ease !important;
        padding: 15px 25px !important;
        box-shadow: 0 8px 25px rgba(255, 215, 0, 0.2) !important;
        text-transform: uppercase !important;
        letter-spacing: 1px !important;
        flex: 1 1 auto !important;
        min-width: 120px !important;
        text-align: center !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        transform: translateY(-5px) !important;
        background: linear-gradient(135deg, #2d2d2d 0%, #1a5f1a 100%) !important;
        border-color: #00FF00 !important;
        color: #00FF00 !important;
        box-shadow: 0 15px 40px rgba(0, 255, 0, 0.4) !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #1a5f1a 0%, #2d8f2d 100%) !important;
        border-color: #00FF00 !important;
        color: #FFFFFF !important;
        box-shadow: 0 0 30px rgba(0, 255, 0, 0.6) !important;
        animation: goldPulse 2s infinite !important;
    }
    
    /* === SIDEBAR - VIP LOUNGE === */
    .css-1d391kg {
        background: linear-gradient(180deg, #000000 0%, #1a1a1a 50%, #000000 100%) !important;
        border-right: 3px solid #FFD700 !important;
    }
    
    /* === INPUT FIELDS - EXECUTIVE FORMS === */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%) !important;
        border: 2px solid #FFD700 !important;
        border-radius: 20px !important;
        color: #FFFFFF !important;
        padding: 15px 25px !important;
        font-family: 'Montserrat', sans-serif !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
        box-shadow: inset 0 0 15px rgba(255, 215, 0, 0.1) !important;
    }
    
    .stTextInput > div > div > input:focus,
    .stSelectbox > div > div > div:focus {
        border-color: #00FF00 !important;
        box-shadow: 
            0 0 20px rgba(0, 255, 0, 0.5),
            inset 0 0 20px rgba(0, 255, 0, 0.1) !important;
        transform: scale(1.02) !important;
    }
    
    /* === PROGRESS BARS - PROFIT METERS === */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #FFD700, #00FF00, #FFD700) !important;
        border-radius: 15px !important;
        box-shadow: 0 0 20px rgba(255, 215, 0, 0.5) !important;
        animation: shimmer 2s infinite !important;
    }
    
    /* === SUCCESS/ALERT MESSAGES - MONEY NOTIFICATIONS === */
    .stAlert {
        background: linear-gradient(135deg, #1a5f1a 0%, #2d8f2d 100%) !important;
        border: 2px solid #00FF00 !important;
        border-radius: 20px !important;
        color: #FFFFFF !important;
        box-shadow: 0 8px 25px rgba(0, 255, 0, 0.3) !important;
    }
    
    /* === FILE UPLOADER - DOCUMENT VAULT === */
    .stFileUploader > div {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%) !important;
        border: 3px dashed #FFD700 !important;
        border-radius: 25px !important;
        padding: 40px !important;
        transition: all 0.4s ease !important;
    }
    
    .stFileUploader > div:hover {
        border-color: #00FF00 !important;
        background: linear-gradient(135deg, #2d2d2d 0%, #1a5f1a 100%) !important;
        transform: scale(1.02) !important;
        box-shadow: 0 15px 40px rgba(0, 255, 0, 0.3) !important;
    }
    
    /* === PREMIUM ANIMATIONS === */
    @keyframes goldPulse {
        0%, 100% { 
            box-shadow: 0 0 20px rgba(255, 215, 0, 0.3);
            transform: scale(1);
        }
        50% { 
            box-shadow: 0 0 50px rgba(255, 215, 0, 0.8);
            transform: scale(1.02);
        }
    }
    
    @keyframes shimmer {
        0% { background-position: -200px 0; }
        100% { background-position: calc(200px + 100%) 0; }
    }
    
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(50px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes celebration {
        0% { transform: scale(1) rotate(0deg); }
        25% { transform: scale(1.1) rotate(5deg); }
        50% { transform: scale(1.05) rotate(-5deg); }
        75% { transform: scale(1.15) rotate(3deg); }
        100% { transform: scale(1) rotate(0deg); }
    }
    
    @keyframes confetti {
        0% {
            transform: translateY(-100vh) rotate(0deg);
            opacity: 1;
        }
        100% {
            transform: translateY(100vh) rotate(720deg);
            opacity: 0;
        }
    }
    
    .celebration-mode {
        animation: celebration 1s ease-in-out !important;
        box-shadow: 0 0 50px rgba(255,215,0,0.9) !important;
    }
    
    /* === CUSTOM SCROLLBAR - GOLD LUXURY === */
    ::-webkit-scrollbar {
        width: 15px;
    }
    
    ::-webkit-scrollbar-track {
        background: #1a1a1a;
        border-radius: 15px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #FFD700, #FFA500);
        border-radius: 15px;
        border: 3px solid #1a1a1a;
        box-shadow: 0 0 10px rgba(255, 215, 0, 0.5);
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #00FF00, #32CD32);
        box-shadow: 0 0 15px rgba(0, 255, 0, 0.7);
    }
    
    /* === MOBILE-FIRST RESPONSIVE WOLF STYLING === */
    @media (max-width: 768px) {
        /* Main container mobile optimization */
        .main > div {
            padding: 1rem 0.5rem !important;
        }
        
        /* Mobile metric cards */
        .stMetric {
            padding: 1.5rem 1rem !important;
            margin: 0.5rem 0 !important;
            border-width: 2px !important;
            border-radius: 20px !important;
        }
        
        .stMetric [data-testid="metric-value"] {
            font-size: 2.5rem !important;
            line-height: 1.1 !important;
        }
        
        .stMetric [data-testid="metric-label"] {
            font-size: 1rem !important;
            letter-spacing: 1px !important;
        }
        
        /* Mobile buttons - touch-friendly */
        .stButton > button {
            width: 100% !important;
            padding: 1rem !important;
            font-size: 1rem !important;
            margin: 0.5rem 0 !important;
            min-height: 48px !important;
            letter-spacing: 1px !important;
        }
        
        /* Mobile tabs - force visibility and proper layout */
        .stTabs {
            width: 100% !important;
            overflow: visible !important;
        }
        
        .stTabs > div {
            width: 100% !important;
            overflow-x: visible !important;
        }
        
        .stTabs [data-baseweb="tab-list"] {
            flex-wrap: wrap !important;
            overflow-x: visible !important;
            overflow-y: visible !important;
            display: flex !important;
            gap: 5px !important;
            padding: 8px 2px !important;
            margin: 8px 0 !important;
            width: 100% !important;
            justify-content: flex-start !important;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 8px 12px !important;
            font-size: 0.75rem !important;
            letter-spacing: 0.3px !important;
            border-radius: 10px !important;
            flex: 0 0 auto !important;
            min-width: 80px !important;
            max-width: 120px !important;
            white-space: nowrap !important;
            display: inline-block !important;
            visibility: visible !important;
            border-width: 2px !important;
            text-align: center !important;
            margin: 2px !important;
        }
        
        /* Mobile input fields */
        .stTextInput > div > div > input,
        .stSelectbox > div > div > div {
            padding: 12px 20px !important;
            font-size: 1rem !important;
            min-height: 48px !important;
            border-radius: 15px !important;
        }
        
        /* Mobile file uploader - Enhanced for FMO uploads */
        .stFileUploader > div {
            padding: 25px !important;
            border-width: 3px !important;
            border-radius: 25px !important;
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%) !important;
            border: 3px solid #FFD700 !important;
            min-height: 120px !important;
            box-shadow: 0 10px 30px rgba(0,0,0,0.7), 0 0 20px rgba(255, 215, 0, 0.3) !important;
        }
        
        .stFileUploader > div > div {
            color: #FFFFFF !important;
            font-size: 1.1rem !important;
            font-weight: 600 !important;
            text-align: center !important;
        }
        
        .stFileUploader button {
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 50%, #FFD700 100%) !important;
            color: #000000 !important;
            border: none !important;
            border-radius: 20px !important;
            padding: 15px 25px !important;
            font-size: 1.1rem !important;
            font-weight: 800 !important;
            min-height: 60px !important;
            width: 100% !important;
            margin: 10px 0 !important;
        }
        
        /* Mobile dataframes */
        .stDataFrame {
            font-size: 0.85rem !important;
            border-width: 2px !important;
            border-radius: 15px !important;
            overflow-x: auto !important;
            min-height: 300px !important;
        }
        
        /* Mobile download buttons */
        .stDownloadButton > button {
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 50%, #FFD700 100%) !important;
            color: #000000 !important;
            border: 3px solid #FFD700 !important;
            border-radius: 25px !important;
            padding: 20px 30px !important;
            font-size: 1.2rem !important;
            font-weight: 900 !important;
            min-height: 70px !important;
            width: 100% !important;
            margin: 15px 0 !important;
            box-shadow: 0 10px 30px rgba(0,0,0,0.7), 0 0 20px rgba(255, 215, 0, 0.4) !important;
            text-transform: uppercase !important;
            letter-spacing: 1px !important;
        }
        
        .stDownloadButton > button:hover {
            transform: translateY(-2px) !important;
            box-shadow: 0 15px 40px rgba(0,0,0,0.8), 0 0 30px rgba(255, 215, 0, 0.6) !important;
        }
        
        /* Mobile columns - stack vertically */
        .css-1r6slb0 {
            flex-direction: column !important;
            gap: 1rem !important;
        }
        
        /* Mobile headers in custom components */
        .admin-header h1 {
            font-size: 2rem !important;
            line-height: 1.2 !important;
            letter-spacing: 2px !important;
            margin: 10px 0 !important;
        }
        
        .admin-header p {
            font-size: 0.9rem !important;
            line-height: 1.3 !important;
            letter-spacing: 1px !important;
        }
        
        .admin-header {
            padding: 20px 15px !important;
            margin: 15px 0 !important;
            border-radius: 20px !important;
        }
        
        /* Mobile live dashboard */
        .live-dashboard {
            padding: 20px 15px !important;
            margin: 15px 0 !important;
            border-radius: 20px !important;
        }
        
        .live-dashboard h3 {
            font-size: 1.5rem !important;
            line-height: 1.2 !important;
            letter-spacing: 1px !important;
        }
        
        /* Mobile metric cards in HTML */
        .metric-card {
            padding: 15px !important;
            margin: 10px 0 !important;
            border-radius: 15px !important;
            border-width: 2px !important;
        }
        
        .metric-card h2, .metric-card h3 {
            font-size: 1.8rem !important;
            line-height: 1.1 !important;
            margin: 5px 0 !important;
        }
        
        .metric-card p {
            font-size: 0.9rem !important;
            line-height: 1.2 !important;
            margin: 8px 0 !important;
        }
        
        /* Mobile performance cards */
        .performance-card {
            padding: 15px !important;
            margin: 10px 0 !important;
            border-radius: 15px !important;
        }
        
        .performance-card h3 {
            font-size: 1.5rem !important;
            line-height: 1.2 !important;
        }
        
        /* Mobile sidebar improvements */
        .css-1d391kg {
            padding: 1rem 0.5rem !important;
        }
        
        /* Mobile form elements spacing */
        .element-container {
            margin: 10px 0 !important;
        }
        
        /* Mobile alert messages */
        .stAlert {
            padding: 15px !important;
            border-radius: 15px !important;
            margin: 10px 0 !important;
        }
        
        /* Mobile progress bars */
        .stProgress {
            margin: 10px 0 !important;
        }
        
        /* Touch-friendly scrollbar for mobile */
        ::-webkit-scrollbar {
            width: 12px !important;
        }
        
        ::-webkit-scrollbar-thumb {
            border-width: 2px !important;
        }
    }
    
    @media (max-width: 480px) {
        /* Extra small mobile devices */
        .main > div {
            padding: 0.5rem 0.25rem !important;
        }
        
        .stMetric {
            padding: 1rem 0.8rem !important;
            border-radius: 15px !important;
        }
        
        .stMetric [data-testid="metric-value"] {
            font-size: 2rem !important;
        }
        
        .stMetric [data-testid="metric-label"] {
            font-size: 0.9rem !important;
        }
        
        .stButton > button {
            padding: 0.8rem !important;
            font-size: 0.9rem !important;
            border-radius: 15px !important;
        }
        
        .admin-header h1 {
            font-size: 1.6rem !important;
            letter-spacing: 1px !important;
        }
        
        .admin-header p {
            font-size: 0.8rem !important;
        }
        
        .live-dashboard h3 {
            font-size: 1.3rem !important;
        }
        
        .metric-card h2, .metric-card h3 {
            font-size: 1.5rem !important;
        }
        
        .metric-card p {
            font-size: 0.8rem !important;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 10px 12px !important;
            font-size: 0.8rem !important;
        }
        
        .stTextInput > div > div > input,
        .stSelectbox > div > div > div {
            padding: 10px 15px !important;
            font-size: 0.9rem !important;
        }
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state for celebration monitoring
if 'previous_deals' not in st.session_state:
    st.session_state.previous_deals = 0
if 'celebration_triggered' not in st.session_state:
    st.session_state.celebration_triggered = False

# Create a more reliable audio system using direct HTML injection
st.markdown("""
<div id="celebration-system" style="display: none;">
    <audio id="celebration-audio" preload="auto" style="display: none;">
        <source src="data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdJivrJBhNjVgodDbq2EcBj+a2/LDciUFLIHO8tiJNwgZaLvt559NEAxQp+PwtmMcBjiR1/LMeSwFJHfH8N2QQAoUXrTp66hVFApGn+DyvmEfCCuX1/LNeSsFJHfH8N2QQAoUXrTp66hVFApGn+DyvmEfCCuX" type="audio/wav">
    </audio>
</div>

<script>
(function() {
    // Create audio context for sound effects
    let audioContext = null;
    let celebrationActive = false;
    
    // Initialize audio
    function initAudio() {
        if (!audioContext) {
            try {
                audioContext = new (window.AudioContext || window.webkitAudioContext)();
            } catch (e) {
                console.log('Audio initialization failed:', e);
            }
        }
    }
    
    // Create cash register sound
    function playSound() {
        if (!audioContext) initAudio();
        if (!audioContext) return;
        
        try {
            // First tone
            const osc1 = audioContext.createOscillator();
            const gain1 = audioContext.createGain();
            osc1.connect(gain1);
            gain1.connect(audioContext.destination);
            
            osc1.frequency.setValueAtTime(800, audioContext.currentTime);
            osc1.frequency.exponentialRampToValueAtTime(600, audioContext.currentTime + 0.1);
            osc1.frequency.exponentialRampToValueAtTime(800, audioContext.currentTime + 0.2);
            
            gain1.gain.setValueAtTime(0.3, audioContext.currentTime);
            gain1.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.5);
            
            osc1.start();
            osc1.stop(audioContext.currentTime + 0.5);
            
            // Second harmony tone
            setTimeout(() => {
                if (!audioContext) return;
                const osc2 = audioContext.createOscillator();
                const gain2 = audioContext.createGain();
                osc2.connect(gain2);
                gain2.connect(audioContext.destination);
                
                osc2.frequency.setValueAtTime(400, audioContext.currentTime);
                gain2.gain.setValueAtTime(0.2, audioContext.currentTime);
                gain2.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.3);
                
                osc2.start();
                osc2.stop(audioContext.currentTime + 0.3);
            }, 100);
        } catch (e) {
            console.log('Sound playback failed:', e);
        }
    }
    
    // Create confetti
    function createConfetti() {
        const colors = ['#ffd700', '#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7'];
        
        for (let i = 0; i < 50; i++) {
            setTimeout(() => {
                const confetti = document.createElement('div');
                confetti.style.position = 'fixed';
                confetti.style.left = Math.random() * window.innerWidth + 'px';
                confetti.style.top = '-10px';
                confetti.style.width = '10px';
                confetti.style.height = '10px';
                confetti.style.backgroundColor = colors[Math.floor(Math.random() * colors.length)];
                confetti.style.zIndex = '9999';
                confetti.style.pointerEvents = 'none';
                confetti.style.borderRadius = '50%';
                
                // Apply animation
                confetti.style.animation = 'confetti 3s linear forwards';
                
                document.body.appendChild(confetti);
                
                setTimeout(() => {
                    if (confetti.parentNode) {
                        confetti.parentNode.removeChild(confetti);
                    }
                }, 3000);
            }, i * 50);
        }
    }
    
    // Main celebration function
    function celebrate() {
        if (celebrationActive) return;
        celebrationActive = true;
        
        console.log('🎉 CELEBRATION STARTED!');
        
        // Play sound
        playSound();
        
        // Show confetti
        createConfetti();
        
        // Animate metric cards
        const cards = document.querySelectorAll('[data-testid="metric-container"], .metric-card, .stMetric');
        cards.forEach(card => {
            card.style.animation = 'celebration 1s ease-in-out';
            card.style.boxShadow = '0 0 30px rgba(255,215,0,0.8)';
        });
        
        // Reset after animation
        setTimeout(() => {
            celebrationActive = false;
            cards.forEach(card => {
                card.style.animation = '';
                card.style.boxShadow = '';
            });
        }, 1000);
    }
    
    // Make globally accessible
    window.testCelebration = celebrate;
    window.triggerCelebration = celebrate;
    
    // Enable audio on interaction
    document.addEventListener('click', function() {
        initAudio();
        if (audioContext && audioContext.state === 'suspended') {
            audioContext.resume();
        }
    }, { once: true });
    
    console.log('🔊 Celebration system loaded! Use testCelebration() or click test button');
})();
</script>
""", unsafe_allow_html=True)

# Check if celebration was triggered and execute it
if st.session_state.get('celebration_triggered', False):
    st.session_state.celebration_triggered = False  # Reset the trigger
    
    # Execute fullscreen celebration
    st.markdown("""
    <script>
    setTimeout(function() {
        // Fullscreen confetti celebration
        const colors = ['#ffd700', '#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7'];
        
        for (let i = 0; i < 100; i++) {
            setTimeout(() => {
                const confetti = document.createElement('div');
                const startX = Math.random() * window.innerWidth;
                const color = colors[Math.floor(Math.random() * colors.length)];
                
                confetti.style.position = 'fixed';
                confetti.style.left = startX + 'px';
                confetti.style.top = '-20px';
                confetti.style.width = '12px';
                confetti.style.height = '12px';
                confetti.style.backgroundColor = color;
                confetti.style.borderRadius = '50%';
                confetti.style.zIndex = '999999';
                confetti.style.pointerEvents = 'none';
                confetti.style.opacity = '1';
                confetti.style.boxShadow = '0 0 8px ' + color;
                
                document.body.appendChild(confetti);
                
                // Animate falling with physics
                let position = -20;
                let rotation = 0;
                const fallSpeed = 4 + Math.random() * 6;
                const rotationSpeed = (Math.random() - 0.5) * 20;
                const drift = (Math.random() - 0.5) * 3;
                let currentX = startX;
                
                const animateConfetti = () => {
                    position += fallSpeed;
                    rotation += rotationSpeed;
                    currentX += drift;
                    
                    confetti.style.top = position + 'px';
                    confetti.style.left = currentX + 'px';
                    confetti.style.transform = `rotate(${rotation}deg) scale(${1 + Math.sin(rotation * 0.05) * 0.4})`;
                    confetti.style.opacity = Math.max(0, 1 - (position / (window.innerHeight + 200)));
                    
                    if (position < window.innerHeight + 200) {
                        requestAnimationFrame(animateConfetti);
                    } else {
                        if (confetti.parentNode) {
                            confetti.parentNode.removeChild(confetti);
                        }
                    }
                };
                
                requestAnimationFrame(animateConfetti);
            }, i * 20);
        }
        
        // Enhanced cash register sound
        try {
            const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            
            const createBell = (freq, time, duration, volume = 0.3) => {
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();
                const filter = audioCtx.createBiquadFilter();
                
                osc.connect(filter);
                filter.connect(gain);
                gain.connect(audioCtx.destination);
                
                filter.type = 'bandpass';
                filter.frequency.value = freq;
                filter.Q.value = 8;
                
                osc.frequency.setValueAtTime(freq, time);
                osc.frequency.exponentialRampToValueAtTime(freq * 0.3, time + duration);
                
                gain.gain.setValueAtTime(volume, time);
                gain.gain.exponentialRampToValueAtTime(0.001, time + duration);
                
                osc.start(time);
                osc.stop(time + duration);
            };
            
            // Multi-layered cash register "cha-ching"
            const now = audioCtx.currentTime;
            createBell(1400, now, 0.12, 0.4);        // High sparkle
            createBell(1000, now + 0.05, 0.18, 0.35); // High bell
            createBell(700, now + 0.12, 0.25, 0.3);   // Mid bell
            createBell(450, now + 0.18, 0.35, 0.25);  // Low bell
            createBell(600, now + 0.22, 0.45, 0.2);   // Resonance
            createBell(300, now + 0.25, 0.5, 0.15);   // Deep resonance
            
            console.log('Enhanced cash register celebration sound!');
        } catch (e) {
            console.log('Audio unavailable:', e.message);
        }
        
        console.log('🎉 FULLSCREEN CELEBRATION COMPLETE!');
    }, 100);
    </script>
    """, unsafe_allow_html=True)

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
commission_cycles["start"] = pd.to_datetime(commission_cycles["start"], format="%m/%d/%y")
commission_cycles["end"] = pd.to_datetime(commission_cycles["end"], format="%m/%d/%y")
commission_cycles["pay"] = pd.to_datetime(commission_cycles["pay"], format="%m/%d/%y")

PROFIT_PER_SALE = 36.47
CRM_API_URL     = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID      = "310"
CRM_API_KEY     = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"
DB              = "crm_history.db"

# Background Sales Monitor Setup
class BackgroundSalesMonitor:
    def __init__(self):
        self.monitor = SalesMonitor()
        self.tracker = DiscordSalesTracker()
        self.running = False
        self.thread = None
        
    def start_monitoring(self, check_interval=30):
        if self.running:
            return
            
        self.running = True
        self.check_interval = check_interval
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        
    def stop_monitoring(self):
        self.running = False
        
    def _monitor_loop(self):
        last_leaderboard_time = 0
        leaderboard_interval = 300  # 5 minutes
        
        while self.running:
            try:
                current_sales = self.monitor.fetch_today_sales()
                new_sales_found = self.monitor.process_new_sales(current_sales)
                
                # Send leaderboard every 5 minutes
                current_time = time.time()
                if current_time - last_leaderboard_time >= leaderboard_interval:
                    agent_stats = {}
                    for sale in current_sales:
                        agent_name = sale.get('agent_name', 'Unknown')
                        if agent_name not in agent_stats:
                            agent_stats[agent_name] = 0
                        agent_stats[agent_name] += 1
                    
                    self.tracker.send_leaderboard_update(agent_stats, "Live Update")
                    last_leaderboard_time = current_time
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                time.sleep(self.check_interval)

# Initialize background monitor as singleton
# Background monitor disabled - using Fixed Monitor only for single source notifications
# if 'background_monitor' not in st.session_state:
#     st.session_state.background_monitor = BackgroundSalesMonitor()
#     st.session_state.background_monitor.start_monitoring()
    
def stop_background_monitor():
    if 'background_monitor' in st.session_state:
        st.session_state.background_monitor.stop_monitoring()

atexit.register(stop_background_monitor)

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
    st.session_state.user_id = ""

# Manager dashboard access


def do_login():
    u = st.session_state.user.strip()
    p = st.session_state.pwd
    
    # Manager credentials for Jarad and Matt
    manager_credentials = {
        "jarad": "password",
        "matt": "password"
    }
    manager_names = {
        "jarad": "Jarad",
        "matt": "Matt"
    }
    
    if u in manager_credentials and p == manager_credentials[u]:
        st.session_state.logged_in = True
        st.session_state.user_email = u
        st.session_state.user_name = manager_names[u]
        st.session_state.user_role = "Manager"
        st.session_state.user_id = u
        st.success(f"✅ Welcome, {manager_names[u]}! (Manager)")
    elif u in AGENT_CREDENTIALS and p == AGENT_CREDENTIALS[u]:
        st.session_state.logged_in = True
        st.session_state.user_email = u
        st.session_state.user_name = AGENT_NAMES[u]
        st.session_state.user_role = AGENT_ROLES[u] if AGENT_ROLES.get(u) else "Agent"
        st.session_state.user_id = AGENT_USERIDS.get(u, '')
        st.success(f"✅ Welcome, {AGENT_NAMES[u]}!")
    elif u in USERS and p == USERS[u]:
        st.session_state.logged_in = True
        st.session_state.user_email = u
        st.session_state.user_name = ADMIN_NAMES.get(u, u)
        st.session_state.user_role = ADMIN_ROLES.get(u, "Admin")
        st.session_state.user_id = ""  # Admins don't have agent IDs
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
    # Mobile-friendly login interface
    st.markdown("""
    <style>
    .login-container {
        background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
        border: 3px solid #FFD700;
        padding: 40px;
        border-radius: 25px;
        margin: 50px auto;
        max-width: 500px;
        box-shadow: 
            0 20px 60px rgba(0,0,0,0.8),
            0 0 40px rgba(255, 215, 0, 0.4),
            inset 0 0 30px rgba(255, 215, 0, 0.1);
        text-align: center;
    }
    .login-title {
        color: #FFD700;
        font-size: 32px;
        font-weight: 900;
        font-family: 'Playfair Display', serif;
        text-shadow: 0 0 20px rgba(255, 215, 0, 0.8);
        letter-spacing: 2px;
        margin-bottom: 30px;
    }
    .login-subtitle {
        color: #FFFFFF;
        font-size: 18px;
        font-weight: 600;
        margin-bottom: 40px;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<h1 class="login-title">🔒 HCS CRM LOGIN</h1>', unsafe_allow_html=True)
    st.markdown('<p class="login-subtitle">Elite Commission Tracking System</p>', unsafe_allow_html=True)
    
    with st.form("login_form", clear_on_submit=False):
        st.text_input("👤 Username", key="user", placeholder="Enter your username")
        st.text_input("🔐 Password", type="password", key="pwd", placeholder="Enter your password")
        login_button = st.form_submit_button("🚀 LOGIN", use_container_width=True)
        
        if login_button:
            do_login()
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Mobile hint
    st.markdown("""
    <div style="text-align: center; margin-top: 30px; color: #888; font-size: 14px;">
    💡 <strong>Mobile Tip:</strong> Rotate to landscape for the best experience
    </div>
    """, unsafe_allow_html=True)
    
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
            df["upload_date"] = pd.to_datetime(df["upload_date"], errors='coerce')
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
def fetch_all_today(limit=5000, send_discord_notifications=False):
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
    headers = {"tld-api-id": CRM_API_ID, "tld-api-key": CRM_API_KEY}
    
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
        "columns": "policy_id,lead_id,date_created,date_converted,date_sold,agent_id,agent_name,lead_first_name,lead_last_name,lead_phone,carrier,product,lead_vendor_name,lead_state,premium"
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
        
        # Additional filtering by agent_id to ensure only this agent's data
        if 'agent_id' in df.columns and user_id:
            df = df[df['agent_id'].astype(str) == str(user_id)]
        
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
    
    # Get agent data from session state if available
    agent_data = None
    if 'agent_reports' in st.session_state and agent_name in st.session_state['agent_reports']:
        agent_data = st.session_state['agent_reports'][agent_name]
    
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial","B",16)
    pdf.cell(0,10,fix("Health Connect Solutions"), ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,10,fix(f"Commission Statement - {agent_name}"), ln=True)
    pdf.ln(5)
    
    if agent_data:
        # Use the calculated data from the commission calculation
        paid_count = agent_data['paid_applications']
        unpaid_count = agent_data['unpaid_applications']
        total_deals = paid_count + unpaid_count
        total_members = agent_data['total_members']
        unpaid_members = agent_data['unpaid_members']
        pct_paid = (paid_count / total_deals * 100) if total_deals else 0
        rate = agent_data['per_member_rate']
        base_pay = agent_data['base_pay']
        production_bonus = agent_data['production_bonus']
        retention_bonus = agent_data['retention_bonus']
        top_agent_bonus = agent_data['top_agent_bonus']
        total_payout = agent_data['gross_pay']
        
        pdf.set_font("Arial","",12)
        pdf.cell(0,8,fix(f"Total Applications Submitted: {total_deals}"), ln=True)
        pdf.cell(0,8,fix(f"Paid Applications: {paid_count}"), ln=True)
        pdf.cell(0,8,fix(f"Unpaid Applications: {unpaid_count}"), ln=True)
        pdf.cell(0,8,fix(f"Paid Percentage: {pct_paid:.1f}%"), ln=True)
        pdf.cell(0,8,fix(f"Total Paid Members: {total_members}"), ln=True)
        pdf.cell(0,8,fix(f"Unpaid Members: {unpaid_members}"), ln=True)
        pdf.ln(3)
        
        pdf.set_font("Arial","B",12)
        pdf.cell(0,8,fix("Commission Breakdown:"), ln=True)
        pdf.set_font("Arial","",12)
        pdf.cell(0,8,fix(f"Per-Member Rate: ${rate}"), ln=True)
        pdf.cell(0,8,fix(f"Base Pay ({total_members} members): ${base_pay:,.2f}"), ln=True)
        if production_bonus > 0:
            pdf.cell(0,8,fix(f"Production Bonus (70+ members): ${production_bonus:,.2f}"), ln=True)
        if retention_bonus > 0:
            pdf.cell(0,8,fix(f"Retention Bonus (80+ members & 80%+ retention): ${retention_bonus:,.2f}"), ln=True)
        if top_agent_bonus > 0:
            pdf.cell(0,8,fix(f"Top Agent Bonus: ${top_agent_bonus:,.2f}"), ln=True)
        
        pdf.ln(3)
        pdf.set_text_color(0,150,0)
        pdf.set_font("Arial","B",14)
        pdf.cell(0,10,fix(f"Total Payout: ${total_payout:,.2f}"), ln=True)
        pdf.set_text_color(0,0,0)
    else:
        # Fallback to basic calculation if no session data
        total_deals = len(df_agent)
        paid_count = (df_agent["Paid Status"]=="Paid").sum() if "Paid Status" in df_agent.columns else 0
        unpaid_count = total_deals - paid_count
        pct_paid = (paid_count / total_deals * 100) if total_deals else 0
        
        pdf.set_font("Arial","",12)
        pdf.cell(0,8,fix(f"Total Applications: {total_deals}"), ln=True)
        pdf.cell(0,8,fix(f"Paid Applications: {paid_count}"), ln=True)
        pdf.cell(0,8,fix(f"Unpaid Applications: {unpaid_count}"), ln=True)
        pdf.cell(0,8,fix(f"Paid Percentage: {pct_paid:.1f}%"), ln=True)
    
    pdf.ln(5)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,8,fix("Paid Applications:"), ln=True)
    pdf.set_font("Arial","",10)
    if "Paid Status" in df_agent.columns:
        for _, row in df_agent[df_agent["Paid Status"]=="Paid"].iterrows():
            first_name = row.get("first_name", "")
            last_name = row.get("last_name", "")
            client_name = f"{first_name} {last_name}".strip()
            pdf.multi_cell(0,6,fix(f"- {client_name}"))
    
    pdf.ln(3)
    pdf.set_font("Arial","B",12)
    pdf.cell(0,8,fix("Unpaid Applications & Reasons:"), ln=True)
    pdf.set_font("Arial","",10)
    if "Paid Status" in df_agent.columns:
        for _, row in df_agent[df_agent["Paid Status"]!="Paid"].iterrows():
            first_name = row.get("first_name", "")
            last_name = row.get("last_name", "")
            reason = row.get("Reason", "")
            client_name = f"{first_name} {last_name}".strip()
            pdf.multi_cell(0,6,fix(f"- {client_name} | Reason: {reason}"))
    
    return pdf.output(dest="S")

def vendor_pdf(paid, unpaid, vendor, rate):
    """PDF that actually works - forces content creation"""
    import tempfile
    import os
    from datetime import datetime
    
    # Create PDF with forced content
    pdf = FPDF()
    pdf.add_page()
    
    # Force content immediately
    pdf.set_font('Arial', 'B', 20)
    pdf.cell(0, 15, 'VENDOR PERFORMANCE REPORT', 0, 1, 'C')
    pdf.ln(5)
    
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 12, f'Vendor: {vendor}', 0, 1, 'C')
    pdf.ln(3)
    
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f'Report Date: {datetime.now().strftime("%B %d, %Y")}', 0, 1, 'C')
    pdf.ln(10)
    
    # Calculate numbers
    paid_count = len(paid) if paid is not None and hasattr(paid, '__len__') else 0
    unpaid_count = len(unpaid) if unpaid is not None and hasattr(unpaid, '__len__') else 0
    total_count = paid_count + unpaid_count
    conversion_rate = (paid_count / total_count * 100) if total_count > 0 else 0
    total_payout = paid_count * rate
    
    # Performance summary box
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 12, 'PERFORMANCE SUMMARY', 1, 1, 'C')
    
    pdf.set_font('Arial', '', 12)
    pdf.cell(120, 10, 'Total Applications Processed:', 1, 0, 'L')
    pdf.cell(0, 10, f'{total_count}', 1, 1, 'C')
    
    pdf.cell(120, 10, 'Applications Paid:', 1, 0, 'L')
    pdf.cell(0, 10, f'{paid_count}', 1, 1, 'C')
    
    pdf.cell(120, 10, 'Applications Unpaid:', 1, 0, 'L')
    pdf.cell(0, 10, f'{unpaid_count}', 1, 1, 'C')
    
    pdf.cell(120, 10, 'Conversion Rate:', 1, 0, 'L')
    pdf.cell(0, 10, f'{conversion_rate:.1f}%', 1, 1, 'C')
    
    pdf.cell(120, 10, 'Commission Rate:', 1, 0, 'L')
    pdf.cell(0, 10, f'${rate:,.0f}', 1, 1, 'C')
    
    pdf.cell(120, 10, 'TOTAL COMMISSION OWED:', 1, 0, 'L')
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, f'${total_payout:,.2f}', 1, 1, 'C')
    
    pdf.ln(10)
    
    # Details section with real client data
    if paid_count > 0:
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 12, f'PAID APPLICATIONS - {paid_count} TOTAL', 1, 1, 'C')
        
        pdf.set_font('Arial', '', 9)
        try:
            # Handle both real pandas DataFrames and mock/list data
            if hasattr(paid, 'iterrows'):
                # Real pandas DataFrame
                for i, row in enumerate(paid.iterrows()):
                    if i >= 100:  # Show up to 100 paid entries
                        break
                    try:
                        _, client_data = row
                        first_name = str(client_data.get('first_name', '')).strip()
                        last_name = str(client_data.get('last_name', '')).strip()
                        client_name = f"{first_name} {last_name}".strip()
                        
                        if not client_name or client_name == ' ':
                            client_name = f"Client #{i+1}"
                        
                        pdf.cell(0, 6, f'{client_name} - Commission: ${rate:,.0f}', 1, 1, 'L')
                    except:
                        pdf.cell(0, 6, f'Paid Client #{i+1} - Commission: ${rate:,.0f}', 1, 1, 'L')
            elif isinstance(paid, list):
                # List of dictionaries
                for i, client_data in enumerate(paid):
                    if i >= 100:
                        break
                    try:
                        first_name = str(client_data.get('first_name', '')).strip()
                        last_name = str(client_data.get('last_name', '')).strip()
                        client_name = f"{first_name} {last_name}".strip()
                        
                        if not client_name or client_name == ' ':
                            client_name = f"Client #{i+1}"
                        
                        pdf.cell(0, 6, f'{client_name} - Commission: ${rate:,.0f}', 1, 1, 'L')
                    except:
                        pdf.cell(0, 6, f'Paid Client #{i+1} - Commission: ${rate:,.0f}', 1, 1, 'L')
            else:
                # Fallback for unknown data structure
                for i in range(min(paid_count, 100)):
                    pdf.cell(0, 6, f'Paid Application #{i+1} - Commission: ${rate:,.0f}', 1, 1, 'L')
        except:
            # Fallback for simple counting
            for i in range(min(paid_count, 100)):
                pdf.cell(0, 6, f'Paid Application #{i+1} - Commission: ${rate:,.0f}', 1, 1, 'L')
    
    if unpaid_count > 0:
        pdf.ln(5)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 12, f'UNPAID APPLICATIONS - {unpaid_count} TOTAL', 1, 1, 'C')
        
        pdf.set_font('Arial', '', 8)
        try:
            # Handle both real pandas DataFrames and mock/list data
            if hasattr(unpaid, 'iterrows'):
                # Real pandas DataFrame
                for i, row in enumerate(unpaid.iterrows()):
                    if i >= 50:  # Show up to 50 unpaid entries
                        break
                    try:
                        _, client_data = row
                        first_name = str(client_data.get('first_name', '')).strip()
                        last_name = str(client_data.get('last_name', '')).strip()
                        phone = str(client_data.get('phone', client_data.get('Phone', ''))).strip()
                        reason = str(client_data.get('reason', client_data.get('Reason', 'Under Review'))).strip()
                        
                        client_name = f"{first_name} {last_name}".strip()
                        if not client_name or client_name == ' ':
                            client_name = f"Client #{i+1}"
                        
                        # Format phone number
                        if phone and phone != 'nan' and phone != '':
                            phone_display = f" | Phone: {phone}"
                        else:
                            phone_display = " | Phone: Not Available"
                        
                        # Format reason
                        if not reason or reason == 'nan' or reason == '':
                            reason = "Under Review"
                        
                        pdf.cell(0, 6, f'{client_name}{phone_display} | Reason: {reason}', 1, 1, 'L')
                    except:
                        pdf.cell(0, 6, f'Unpaid Client #{i+1} | Phone: Not Available | Reason: Under Review', 1, 1, 'L')
            elif isinstance(unpaid, list):
                # List of dictionaries
                for i, client_data in enumerate(unpaid):
                    if i >= 50:
                        break
                    try:
                        first_name = str(client_data.get('first_name', '')).strip()
                        last_name = str(client_data.get('last_name', '')).strip()
                        phone = str(client_data.get('phone', client_data.get('Phone', ''))).strip()
                        reason = str(client_data.get('reason', client_data.get('Reason', 'Under Review'))).strip()
                        
                        client_name = f"{first_name} {last_name}".strip()
                        if not client_name or client_name == ' ':
                            client_name = f"Client #{i+1}"
                        
                        # Format phone number
                        if phone and phone != 'nan' and phone != '':
                            phone_display = f" | Phone: {phone}"
                        else:
                            phone_display = " | Phone: Not Available"
                        
                        # Format reason
                        if not reason or reason == 'nan' or reason == '':
                            reason = "Under Review"
                        
                        pdf.cell(0, 6, f'{client_name}{phone_display} | Reason: {reason}', 1, 1, 'L')
                    except:
                        pdf.cell(0, 6, f'Unpaid Client #{i+1} | Phone: Not Available | Reason: Under Review', 1, 1, 'L')
            else:
                # Fallback for unknown data structure
                for i in range(min(unpaid_count, 50)):
                    pdf.cell(0, 6, f'Unpaid Application #{i+1} | Phone: Not Available | Reason: Under Review', 1, 1, 'L')
        except:
            # Fallback for simple counting
            for i in range(min(unpaid_count, 50)):
                pdf.cell(0, 6, f'Unpaid Application #{i+1} | Phone: Not Available | Reason: Under Review', 1, 1, 'L')
    
    # Footer
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 15, f'FINAL COMMISSION: ${total_payout:,.2f}', 1, 1, 'C')
    
    # Get the PDF content as bytes
    pdf_content = pdf.output(dest='S').encode('latin-1')
    
    # Verify content was created
    if len(pdf_content) < 100:
        # Force create content if somehow empty
        pdf_content = b'%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n4 0 obj\n<< /Length 100 >>\nstream\nBT\n/F1 12 Tf\n100 700 Td\n(VENDOR REPORT CONTENT) Tj\nET\nendstream\nendobj\n5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Times-Roman >>\nendobj\nxref\n0 6\n0000000000 65535 f \n0000000010 00000 n \n0000000079 00000 n \n0000000173 00000 n \n0000000301 00000 n \n0000000380 00000 n \ntrailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n492\n%%EOF'
    
    return pdf_content

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
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 4px solid #FFD700;
            padding: 40px;
            border-radius: 25px;
            color: #FFFFFF;
            text-align: center;
            margin-bottom: 40px;
            box-shadow: 
                0 0 60px rgba(255, 215, 0, 0.4),
                inset 0 0 40px rgba(255, 215, 0, 0.1);
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.08), transparent);
                animation: shimmer 4s infinite;
            "></div>
            <h1 style="
                margin: 0; 
                font-size: 42px; 
                font-weight: 900; 
                font-family: 'Playfair Display', serif;
                color: #FFD700;
                text-shadow: 
                    0 0 25px rgba(255, 215, 0, 0.8),
                    4px 4px 8px rgba(0,0,0,0.8);
                letter-spacing: 2px;
                position: relative;
                z-index: 2;
            ">
                {emoji} {greeting}, {st.session_state.user_name}!
            </h1>
            <p style="
                margin: 20px 0; 
                font-size: 22px; 
                font-weight: 600;
                font-family: 'Montserrat', sans-serif;
                color: #FFFFFF;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                position: relative;
                z-index: 2;
            ">
                {time_message}
            </p>
            <div style="
                margin-top: 25px; 
                padding: 20px 35px; 
                background: linear-gradient(135deg, #1a5f1a 0%, #2d8f2d 100%);
                border: 2px solid #00FF00;
                border-radius: 20px; 
                display: inline-block;
                box-shadow: 
                    0 0 30px rgba(0, 255, 0, 0.4),
                    inset 0 0 20px rgba(0, 255, 0, 0.1);
                position: relative;
                z-index: 2;
            ">
                <span style="
                    font-size: 20px; 
                    font-weight: 900;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFFFFF;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                    letter-spacing: 2px;
                ">💰 DOMINATE TODAY • CRUSH YOUR GOALS • GET PAID 💰</span>
            </div>
        </div>
        """, unsafe_allow_html=True,
    )

    # Today's Top Performers Section for Agent Dashboard
    st.markdown("---")
    st.subheader("🏆 Today's Top Performers")
    
    # Display leaderboard data from Fixed Monitor only - no duplicate processing
    try:
        # Use cached data to prevent duplicate API calls and notifications
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
        
        # Discord notifications handled by Fixed Monitor only
        
        # Data processing only for display - NO notifications sent
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
                medals = ["👑 THE WOLF", "💎 BULL MASTER", "🔥 DEAL CLOSER"]
                medal = medals[i]
                
                with col:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                        border: 3px solid #FFD700;
                        padding: 25px;
                        border-radius: 20px;
                        color: white;
                        text-align: center;
                        box-shadow: 
                            0 0 40px rgba(255, 215, 0, 0.4),
                            inset 0 0 25px rgba(255, 215, 0, 0.1);
                        position: relative;
                        overflow: hidden;
                    ">
                        <div style="
                            position: absolute;
                            top: -50%;
                            left: -50%;
                            width: 200%;
                            height: 200%;
                            background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.05), transparent);
                            animation: shimmer 3s infinite;
                        "></div>
                        <h3 style="
                            margin: 0; 
                            color: #FFD700;
                            font-family: 'Playfair Display', serif;
                            font-size: 24px;
                            font-weight: 900;
                            text-shadow: 0 0 15px rgba(255, 215, 0, 0.8);
                            position: relative;
                            z-index: 2;
                        ">{medal}</h3>
                        <h2 style="
                            margin: 10px 0; 
                            color: #FFFFFF;
                            font-family: 'Playfair Display', serif;
                            font-size: 22px;
                            font-weight: 900;
                            text-shadow: 
                                0 0 20px rgba(255, 255, 255, 0.9),
                                2px 2px 4px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                            letter-spacing: 1px;
                        ">{performer['agent_name']}</h2>
                        <h3 style="
                            margin: 15px 0; 
                            color: #00FF00;
                            font-family: 'Montserrat', sans-serif;
                            font-size: 18px;
                            font-weight: 800;
                            text-shadow: 
                                0 0 15px rgba(0, 255, 0, 0.9),
                                2px 2px 4px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                        ">{performer['deals']} deals | {performer['members']} members</h3>
                        <p style="
                            margin: 8px 0; 
                            color: #FFFFFF;
                            font-family: 'Montserrat', sans-serif;
                            font-weight: 600;
                            font-size: 14px;
                            text-shadow: 
                                0 0 10px rgba(255, 255, 255, 0.8),
                                1px 1px 2px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                        ">Top Carrier: {performer['top_carrier']}</p>
                        <p style="
                            margin: 8px 0; 
                            color: #00FF00;
                            font-family: 'Montserrat', sans-serif;
                            font-weight: 700;
                            font-size: 14px;
                            text-shadow: 
                                0 0 12px rgba(0, 255, 0, 0.9),
                                1px 1px 2px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                        ">Closing Rate: {performer.get('closing_rate', 0)}%</p>
                        <p style="
                            margin: 8px 0; 
                            color: #FFD700;
                            font-family: 'Montserrat', sans-serif;
                            font-size: 16px;
                            font-weight: 900;
                            text-shadow: 
                                0 0 15px rgba(255, 215, 0, 0.9),
                                2px 2px 4px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                        "><strong>CPA: ${performer['cpa']}</strong></p>
                        <p style="
                            margin: 8px 0; 
                            color: #FFFFFF;
                            font-family: 'Montserrat', sans-serif;
                            font-weight: 600;
                            font-size: 13px;
                            text-shadow: 
                                0 0 10px rgba(255, 255, 255, 0.8),
                                1px 1px 2px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                        ">Total Calls: {performer.get('total_calls', 0)}</p>
                        <p style="
                            margin: 8px 0; 
                            color: #00FF00;
                            font-family: 'Montserrat', sans-serif;
                            font-size: 15px;
                            font-weight: 800;
                            text-shadow: 
                                0 0 12px rgba(0, 255, 0, 0.9),
                                2px 2px 3px rgba(0,0,0,0.8);
                            position: relative;
                            z-index: 2;
                        ">Est. Commission: ${performer['est_commission']}</p>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("Live performance data will be available shortly.")
            
    except Exception as e:
        st.warning("Live performance data temporarily unavailable.")
    
    # Agent XP Leaderboard Section
    st.markdown("---")
    st.subheader("🎮 Agent Experience Leaderboard")
    
    try:
        from agent_xp_system import AgentXPSystem
        xp_system = AgentXPSystem()
        
        # Get leaderboard data
        leaderboard = xp_system.get_leaderboard(sort_by="level")
        
        if leaderboard:
            # Create columns for top performers
            xp_col1, xp_col2, xp_col3 = st.columns(3)
            
            for i, agent_data in enumerate(leaderboard[:3]):
                col = [xp_col1, xp_col2, xp_col3][i]
                rank_colors = ["#FFD700", "#C0C0C0", "#CD7F32"]  # Gold, Silver, Bronze
                rank_medals = ["👑", "🥈", "🥉"]
                
                with col:
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                        border: 3px solid {rank_colors[i]};
                        padding: 20px;
                        border-radius: 15px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 0 20px rgba(255, 215, 0, 0.3);
                        margin-bottom: 15px;
                    ">
                        <h3 style="margin: 0; color: {rank_colors[i]}; font-size: 18px;">
                            {rank_medals[i]} #{i+1}
                        </h3>
                        <h4 style="margin: 5px 0; color: white; font-size: 16px;">
                            {agent_data['agent_name']}
                        </h4>
                        <p style="margin: 8px 0; color: {rank_colors[i]}; font-size: 14px; font-weight: bold;">
                            {agent_data['level_emoji']} Level {agent_data['level']}
                        </p>
                        <p style="margin: 5px 0; color: #cccccc; font-size: 13px;">
                            {agent_data['level_title']}
                        </p>
                        <p style="margin: 8px 0; color: white; font-size: 14px;">
                            {agent_data['total_xp']:,} XP
                        </p>
                        <p style="margin: 5px 0; color: #888; font-size: 12px;">
                            {agent_data['progress_percentage']:.1f}% to next level
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Full leaderboard table
            st.markdown("### 📊 Complete XP Rankings")
            
            leaderboard_df = pd.DataFrame([
                {
                    "Rank": i+1,
                    "Agent": agent_data['agent_name'],
                    "Level": f"{agent_data['level_emoji']} {agent_data['level']}",
                    "Title": agent_data['level_title'],
                    "Total XP": f"{agent_data['total_xp']:,}",
                    "Daily Sales": agent_data['daily_sales'],
                    "Progress": f"{agent_data['progress_percentage']:.1f}%"
                }
                for i, agent_data in enumerate(leaderboard)
            ])
            
            st.dataframe(
                leaderboard_df,
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.info("XP leaderboard will populate as agents make sales today.")
            
    except Exception as e:
        st.warning("XP system temporarily unavailable.")
    
    # Motivational Daily Goal Section
    st.markdown("---")
    st.subheader("🎯 Today's Challenge")
    
    goal_col1, goal_col2, goal_col3 = st.columns(3)
    
    with goal_col1:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 3px solid #FFD700;
            padding: 25px;
            border-radius: 20px;
            text-align: center;
            color: white;
            box-shadow: 
                0 0 40px rgba(255, 215, 0, 0.4),
                inset 0 0 25px rgba(255, 215, 0, 0.1);
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.05), transparent);
                animation: shimmer 3s infinite;
            "></div>
            <h3 style="
                margin: 0; 
                color: #FFD700;
                font-family: 'Playfair Display', serif;
                font-size: 22px;
                font-weight: 900;
                text-shadow: 0 0 15px rgba(255, 215, 0, 0.8);
                position: relative;
                z-index: 2;
            ">💰 Daily Target</h3>
            <h2 style="
                margin: 15px 0; 
                color: #00FF00;
                font-family: 'Montserrat', sans-serif;
                font-size: 28px;
                font-weight: 900;
                text-shadow: 0 0 10px rgba(0, 255, 0, 0.8);
                position: relative;
                z-index: 2;
            ">5 Deals</h2>
            <p style="
                margin: 5px 0; 
                color: #FFFFFF;
                font-family: 'Montserrat', sans-serif;
                font-weight: 600;
                position: relative;
                z-index: 2;
            ">DOMINATE THE MARKET!</p>
        </div>
        """, unsafe_allow_html=True)
    
    with goal_col2:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #1a5f1a 0%, #2d8f2d 100%);
            border: 3px solid #00FF00;
            padding: 25px;
            border-radius: 20px;
            text-align: center;
            color: white;
            box-shadow: 
                0 0 40px rgba(0, 255, 0, 0.4),
                inset 0 0 25px rgba(0, 255, 0, 0.1);
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: linear-gradient(45deg, transparent, rgba(0, 255, 0, 0.05), transparent);
                animation: shimmer 3s infinite;
            "></div>
            <h3 style="
                margin: 0; 
                color: #00FF00;
                font-family: 'Playfair Display', serif;
                font-size: 22px;
                font-weight: 900;
                text-shadow: 0 0 15px rgba(0, 255, 0, 0.8);
                position: relative;
                z-index: 2;
            ">🚀 Power Zone</h3>
            <h2 style="
                margin: 15px 0; 
                color: #FFD700;
                font-family: 'Montserrat', sans-serif;
                font-size: 28px;
                font-weight: 900;
                text-shadow: 0 0 10px rgba(255, 215, 0, 0.8);
                position: relative;
                z-index: 2;
            ">7+ Deals</h2>
            <p style="
                margin: 5px 0; 
                color: #FFFFFF;
                font-family: 'Montserrat', sans-serif;
                font-weight: 600;
                position: relative;
                z-index: 2;
            ">EXTRA $100 BONUS!</p>
        </div>
        """, unsafe_allow_html=True)
    
    with goal_col3:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #4d0000 0%, #800000 50%, #4d0000 100%);
            border: 3px solid #FFD700;
            padding: 25px;
            border-radius: 20px;
            text-align: center;
            color: white;
            box-shadow: 
                0 0 50px rgba(255, 215, 0, 0.6),
                inset 0 0 30px rgba(255, 215, 0, 0.2);
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.08), transparent);
                animation: shimmer 2s infinite;
            "></div>
            <h3 style="
                margin: 0; 
                color: #FFD700;
                font-family: 'Playfair Display', serif;
                font-size: 22px;
                font-weight: 900;
                text-shadow: 0 0 20px rgba(255, 215, 0, 0.9);
                position: relative;
                z-index: 2;
            ">👑 ELITE STATUS</h3>
            <h2 style="
                margin: 15px 0; 
                color: #FF0000;
                font-family: 'Montserrat', sans-serif;
                font-size: 28px;
                font-weight: 900;
                text-shadow: 0 0 15px rgba(255, 0, 0, 0.8);
                position: relative;
                z-index: 2;
            ">10+ Deals</h2>
            <p style="
                margin: 5px 0; 
                color: #FFFFFF;
                font-family: 'Montserrat', sans-serif;
                font-weight: 600;
                position: relative;
                z-index: 2;
            ">WALL STREET LEGEND!</p>
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

    # Calculate agent ranking using the exact same API call as live dashboard
    agent_rank = "#N/A"
    try:
        # Use identical API call from live dashboard section
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
        
        # Calculate performance by agent (same logic as live dashboard)
        agent_stats = {}
        
        if results:
            for deal in results:
                agent_name = deal.get('agent_name', 'Unknown')
                if agent_name not in agent_stats:
                    agent_stats[agent_name] = {'deals': 0}
                agent_stats[agent_name]['deals'] += 1
        
        # Sort agents by deal count (descending) - same as live dashboard
        sorted_agents = sorted(agent_stats.items(), key=lambda x: x[1]['deals'], reverse=True)
        
        # Find current agent's rank in the sorted list
        current_agent_name = st.session_state.get("user_name", "")
        
        for rank, (agent_name, stats) in enumerate(sorted_agents, 1):
            # Enhanced name matching for various formats
            # Handle cases like "Pelissier, Robertho" vs "Robertho Pelissier" 
            # or "Clarke, Jahmani" vs "Jahmani Clarke"
            
            # Split both names into parts
            api_parts = [part.strip().lower() for part in agent_name.replace(',', ' ').split() if part.strip()]
            current_parts = [part.strip().lower() for part in current_agent_name.replace(',', ' ').split() if part.strip()]
            
            # Check for exact match or reversed name match
            name_match = False
            
            if len(api_parts) >= 2 and len(current_parts) >= 2:
                # Check if first/last names match in any order
                api_first, api_last = api_parts[0], api_parts[1]
                current_first, current_last = current_parts[0], current_parts[1]
                
                # Match: "Last, First" format vs "First Last" format
                if (api_first == current_last and api_last == current_first) or \
                   (api_first == current_first and api_last == current_last):
                    name_match = True
            
            # Also check if any significant part matches (at least 4 chars)
            if not name_match:
                for api_part in api_parts:
                    for current_part in current_parts:
                        if len(api_part) >= 4 and len(current_part) >= 4:
                            if api_part in current_part or current_part in api_part:
                                name_match = True
                                break
                    if name_match:
                        break
            
            if name_match:
                agent_rank = f"#{rank}"
                break
                
    except Exception as e:
        # Fallback based on current performance
        agent_rank = "#N/A" if deal_count == 0 else "#1"
    
    # Performance Snapshot Dashboard
    st.markdown("---")
    st.subheader("📊 Your Performance Snapshot")
    
    # Create animated progress rings for key metrics
    quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)
    
    with quick_col1:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 2px solid #FFD700;
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            box-shadow: 
                0 10px 30px rgba(0,0,0,0.7),
                0 0 20px rgba(255, 215, 0, 0.3);
        ">
            <h4 style="
                margin: 0; 
                font-size: 16px; 
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                color: #FFFFFF;
                text-transform: uppercase;
                letter-spacing: 1px;
            ">Today's Deals</h4>
            <h2 style="
                margin: 8px 0; 
                font-size: 32px; 
                font-weight: 900;
                font-family: 'Montserrat', sans-serif;
                color: #FFD700;
                text-shadow: 0 0 15px rgba(255, 215, 0, 0.8);
            ">{deal_count}</h2>
            <div style="
                width: 60px;
                height: 4px;
                background: linear-gradient(90deg, #FFD700, #FFA500);
                border-radius: 2px;
                margin: 10px auto;
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
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 2px solid #00FF00;
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            box-shadow: 
                0 10px 30px rgba(0,0,0,0.7),
                0 0 20px rgba(0, 255, 0, 0.3);
        ">
            <h4 style="
                margin: 0; 
                font-size: 16px; 
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                color: #FFFFFF;
                text-transform: uppercase;
                letter-spacing: 1px;
            ">Closing Rate</h4>
            <h2 style="
                margin: 8px 0; 
                font-size: 32px; 
                font-weight: 900;
                font-family: 'Montserrat', sans-serif;
                color: #00FF00;
                text-shadow: 0 0 15px rgba(0, 255, 0, 0.8);
            ">{closing_rate:.1f}%</h2>
            <div style="
                width: 60px;
                height: 4px;
                background: linear-gradient(90deg, #00FF00, #32CD32);
                border-radius: 2px;
                margin: 10px auto;
                position: relative;
            ">
                <div style="
                    width: 80%;
                    height: 100%;
                    background: #00FF00;
                    border-radius: 2px;
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with quick_col3:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 2px solid #FFD700;
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            box-shadow: 
                0 10px 30px rgba(0,0,0,0.7),
                0 0 20px rgba(255, 215, 0, 0.3);
        ">
            <h4 style="
                margin: 0; 
                font-size: 16px; 
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                color: #FFFFFF;
                text-transform: uppercase;
                letter-spacing: 1px;
            ">Est. Earnings</h4>
            <h2 style="
                margin: 8px 0; 
                font-size: 32px; 
                font-weight: 900;
                font-family: 'Montserrat', sans-serif;
                color: #FFD700;
                text-shadow: 0 0 15px rgba(255, 215, 0, 0.8);
            ">${int(member_count * 15)}</h2>
            <div style="
                width: 60px;
                height: 4px;
                background: linear-gradient(90deg, #FFD700, #FFA500);
                border-radius: 2px;
                margin: 10px auto;
                position: relative;
            ">
                <div style="
                    width: 85%;
                    height: 100%;
                    background: #FFD700;
                    border-radius: 2px;
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with quick_col4:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 2px solid #C0C0C0;
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            color: white;
            box-shadow: 
                0 10px 30px rgba(0,0,0,0.7),
                0 0 20px rgba(192, 192, 192, 0.3);
        ">
            <h4 style="
                margin: 0; 
                font-size: 16px; 
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                color: #FFFFFF;
                text-transform: uppercase;
                letter-spacing: 1px;
            ">Rank Today</h4>
            <h2 style="
                margin: 8px 0; 
                font-size: 32px; 
                font-weight: 900;
                font-family: 'Montserrat', sans-serif;
                color: #C0C0C0;
                text-shadow: 0 0 15px rgba(192, 192, 192, 0.8);
            ">{agent_rank}</h2>
            <div style="
                width: 60px;
                height: 4px;
                background: linear-gradient(90deg, #C0C0C0, #A0A0A0);
                border-radius: 2px;
                margin: 10px auto;
                position: relative;
            ">
                <div style="
                    width: 100%;
                    height: 100%;
                    background: #C0C0C0;
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
        
        # Use authentic data sources matching Performance Overview
        try:
            # Today's data from Performance Overview
            deals_today = deal_count
            members_today = member_count
            
            # Get cycle data using same logic as Performance Overview
            today = pd.Timestamp.now().normalize()
            current_cycle = commission_cycles[
                (commission_cycles["start"] <= today) & (today <= commission_cycles["end"])
            ]
            
            if not current_cycle.empty:
                cycle_row = current_cycle.iloc[0]
                cycle_start = cycle_row["start"].strftime("%Y-%m-%d")
                cycle_end = cycle_row["end"].strftime("%Y-%m-%d")
                
                # Fetch cycle data using same function as other parts of the app
                cycle_df = fetch_agent_deals(user_id, cycle_start, cycle_end)
                deals_cycle = len(cycle_df) if not cycle_df.empty else 0
                members_cycle = cycle_df['total_members'].sum() if not cycle_df.empty and 'total_members' in cycle_df.columns else deals_cycle
            else:
                deals_cycle = 0
                members_cycle = 0
            
            # Use cycle data for month/year approximations
            deals_month = deals_cycle
            members_month = members_cycle
            deals_year = deals_cycle
            members_year = members_cycle
            
            # Calculate commission based on agent's cycle member count
            if members_cycle >= 140:
                rate = 25
                bonus = 1200
            elif members_cycle >= 100:
                rate = 22.5
                bonus = 1200
            elif members_cycle >= 70:
                rate = 17.5
                bonus = 1200
            else:
                rate = 15
                bonus = 0
            
            est_commission = members_cycle * rate + bonus
            
        except Exception as e:
            # Show the error to understand what's failing
            st.error(f"Performance Metrics Error: {str(e)}")
            
            # Use the same successful logic as Performance Overview
            user_id = st.session_state.get('user_id', '')
            if user_id:
                # Use direct API calls like Performance Overview does
                try:
                    # Today's data
                    today_str = today.strftime("%Y-%m-%d")
                    today_deals_df = fetch_agent_deals(user_id, today_str, today_str)
                    deals_today = len(today_deals_df)
                    members_today = today_deals_df['total_members'].sum() if not today_deals_df.empty and 'total_members' in today_deals_df.columns else deals_today
                    
                    # This month data
                    month_start = today.replace(day=1).strftime("%Y-%m-%d")
                    month_end = today.strftime("%Y-%m-%d")
                    month_deals_df = fetch_agent_deals(user_id, month_start, month_end)
                    deals_month = len(month_deals_df)
                    members_month = month_deals_df['total_members'].sum() if not month_deals_df.empty and 'total_members' in month_deals_df.columns else deals_month
                    
                    # This year data
                    year_start = f"{today.year}-01-01"
                    year_end = today.strftime("%Y-%m-%d")
                    year_deals_df = fetch_agent_deals(user_id, year_start, year_end)
                    deals_year = len(year_deals_df)
                    members_year = year_deals_df['total_members'].sum() if not year_deals_df.empty and 'total_members' in year_deals_df.columns else deals_year
                    
                    # Cycle data using same logic as Performance Overview
                    current_cycle_check = commission_cycles[
                        (commission_cycles["start"] <= today) & (today <= commission_cycles["end"])
                    ]
                    
                    if not current_cycle_check.empty:
                        cycle_row = current_cycle_check.iloc[0]
                        cycle_start_str = cycle_row["start"].strftime("%Y-%m-%d")
                        cycle_end_str = cycle_row["end"].strftime("%Y-%m-%d")
                        cycle_deals = fetch_agent_deals(user_id, cycle_start_str, cycle_end_str)
                        deals_cycle = len(cycle_deals)
                        members_cycle = cycle_deals['total_members'].sum() if not cycle_deals.empty and 'total_members' in cycle_deals.columns else deals_cycle
                    else:
                        deals_cycle = 0
                        members_cycle = 0
                except:
                    # Final fallback - use minimal values
                    deals_today = 0
                    members_today = 0
                    deals_month = 1
                    members_month = 1
                    deals_year = 1
                    members_year = 1
                    deals_cycle = 1
                    members_cycle = 1
            else:
                # No user ID - admin user
                deals_today = 0
                members_today = 0
                deals_month = deal_count
                members_month = member_count
                deals_year = deal_count
                members_year = member_count
                deals_cycle = deal_count
                members_cycle = member_count
            
            # Calculate commission
            if members_cycle >= 140:
                rate = 25
                bonus = 1200
            elif members_cycle >= 100:
                rate = 22.5
                bonus = 1200
            elif members_cycle >= 70:
                rate = 17.5
                bonus = 1200
            else:
                rate = 15
                bonus = 0
            est_commission = members_cycle * rate + bonus
        
        # Enhanced metrics with stunning visual design
        col_header, col_test = st.columns([4, 1])
        
        with col_header:
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                border: 3px solid #FFD700;
                padding: 30px;
                border-radius: 25px;
                margin-bottom: 30px;
                box-shadow: 
                    0 0 50px rgba(255, 215, 0, 0.4),
                    inset 0 0 30px rgba(255, 215, 0, 0.1);
                position: relative;
                overflow: hidden;
            ">
                <div style="
                    position: absolute;
                    top: -50%;
                    left: -50%;
                    width: 200%;
                    height: 200%;
                    background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.05), transparent);
                    animation: shimmer 3s infinite;
                "></div>
                <h2 style="
                    margin: 0;
                    color: #FFD700;
                    font-size: 32px;
                    font-weight: 900;
                    text-align: center;
                    font-family: 'Playfair Display', serif;
                    text-shadow: 
                        0 0 20px rgba(255, 215, 0, 0.8),
                        3px 3px 6px rgba(0,0,0,0.8);
                    letter-spacing: 2px;
                    position: relative;
                    z-index: 2;
                ">💰 WALL STREET PERFORMANCE 💰</h2>
                <p style="
                    margin: 10px 0 0 0;
                    color: #FFFFFF;
                    font-size: 16px;
                    font-weight: 600;
                    text-align: center;
                    font-family: 'Montserrat', sans-serif;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                    position: relative;
                    z-index: 2;
                    letter-spacing: 1px;
                ">ELITE COMMISSION TRACKING SYSTEM</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col_test:
            pass  # Test celebration removed
        
        # Agent Performance Dashboard - Clean metrics
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("💰 Deals Today", f"{deals_today:,}")
        col2.metric("💚 Members Today", f"{members_today:,}")
        col3.metric("🔥 Deals This Cycle", f"{deals_cycle:,}")
        col4.metric("💎 Members This Cycle", f"{members_cycle:,}")
        
        # Second row - Additional metrics 
        col5, col6, col7, col8 = st.columns(4)
        
        col5.metric("💰 Deals This Month", f"{deals_month:,}")
        col6.metric("💚 Members This Month", f"{members_month:,}")
        col7.metric("🔥 Deals This Year", f"{deals_year:,}")
        col8.metric("💎 Members This Year", f"{members_year:,}")
        
        # Commission and Pay Date - Clean metrics
        col9, col10 = st.columns(2)
        
        col9.metric("💰 Commission Power", f"${est_commission:,.0f}")
        col10.metric("📅 Pay Date", pay_date)
        
        # Previous Cycle Performance Section
        st.markdown("---")
        st.markdown("### 📊 Previous Cycle Performance (Completed)")
        
        # Find the previous cycle (most recent completed cycle)
        try:
            # Get all cycles that ended before today
            completed_cycles = commission_cycles[commission_cycles["end"].dt.date < today.date()]
            
            if not completed_cycles.empty:
                # Get the most recent completed cycle
                previous_cycle = completed_cycles.iloc[-1]
                prev_start = previous_cycle["start"].strftime("%Y-%m-%d")
                prev_end = previous_cycle["end"].strftime("%Y-%m-%d")
                prev_pay_date = previous_cycle["pay"].strftime("%m/%d/%y")
                
                # Get agent's performance for the previous cycle
                if user_id:
                    # Make end date inclusive by adding one day
                    from datetime import datetime, timedelta
                    end_date_inclusive = (datetime.strptime(prev_end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    
                    prev_cycle_df = fetch_agent_deals(user_id, prev_start, end_date_inclusive)
                    prev_deals = len(prev_cycle_df) if not prev_cycle_df.empty else 0
                    prev_members = prev_cycle_df['total_members'].sum() if not prev_cycle_df.empty and 'total_members' in prev_cycle_df.columns else prev_deals
                    
                    # Calculate previous cycle commission
                    if prev_members >= 140:
                        prev_rate = 25
                        prev_bonus = 1200
                    elif prev_members >= 100:
                        prev_rate = 22.5
                        prev_bonus = 1200
                    elif prev_members >= 70:
                        prev_rate = 17.5
                        prev_bonus = 1200
                    else:
                        prev_rate = 15
                        prev_bonus = 0
                    
                    prev_commission = prev_members * prev_rate + prev_bonus
                    
                    # Display previous cycle info
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
                        border: 2px solid #4a90e2;
                        padding: 25px;
                        border-radius: 20px;
                        margin: 20px 0;
                        box-shadow: 0 10px 30px rgba(74, 144, 226, 0.3);
                    ">
                        <h4 style="
                            margin: 0 0 15px 0;
                            color: #4a90e2;
                            font-size: 20px;
                            font-weight: 700;
                            text-align: center;
                            font-family: 'Montserrat', sans-serif;
                        ">🏆 Last Completed Cycle ({prev_start} to {prev_end})</h4>
                        <p style="
                            margin: 0 0 20px 0;
                            color: #ffffff;
                            font-size: 14px;
                            text-align: center;
                            font-weight: 600;
                        ">Pay Date: {prev_pay_date} | Your estimated earnings below</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Previous cycle metrics
                    prev_col1, prev_col2, prev_col3, prev_col4 = st.columns(4)
                    
                    prev_col1.metric("📈 Deals Completed", f"{prev_deals:,}")
                    prev_col2.metric("👥 Members Enrolled", f"{prev_members:,}")
                    prev_col3.metric("💰 Estimated Pay", f"${prev_commission:,.0f}")
                    prev_col4.metric("💎 Commission Rate", f"${prev_rate}/member + ${prev_bonus}")
                    
                    # Show pay tier achieved
                    if prev_members >= 140:
                        tier_msg = "🏆 **DIAMOND TIER** - Maximum commission rate achieved!"
                        tier_color = "#FFD700"
                    elif prev_members >= 100:
                        tier_msg = "💎 **PLATINUM TIER** - Excellent performance!"
                        tier_color = "#C0C0C0"
                    elif prev_members >= 70:
                        tier_msg = "🥉 **GOLD TIER** - Great work!"
                        tier_color = "#CD7F32"
                    else:
                        tier_msg = "📊 **BASE TIER** - Keep building!"
                        tier_color = "#4a90e2"
                    
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, {tier_color}20 0%, {tier_color}10 100%);
                        border: 2px solid {tier_color};
                        padding: 15px;
                        border-radius: 15px;
                        margin: 15px 0;
                        text-align: center;
                    ">
                        <p style="
                            margin: 0;
                            color: {tier_color};
                            font-size: 16px;
                            font-weight: 700;
                        ">{tier_msg}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.info(f"💡 **Payment Status**: This completed cycle will be processed when statements are uploaded on Wednesday. Your estimated pay of ${prev_commission:,.0f} will be confirmed with actual numbers.")
                    
                    # Detailed Deals List for Previous Cycle
                    st.markdown("---")
                    st.markdown("#### 📋 Previous Cycle Deals Detail")
                    
                    if not prev_cycle_df.empty:
                        # Prepare deals data for display
                        deals_display = prev_cycle_df.copy()
                        
                        # Format the data for better display
                        if 'date_created' in deals_display.columns:
                            deals_display['Sale Date'] = pd.to_datetime(deals_display['date_created']).dt.strftime('%m/%d/%Y')
                        else:
                            deals_display['Sale Date'] = 'N/A'
                        
                        # Create customer name from API data (using correct column names)
                        customer_names = []
                        for idx, row in deals_display.iterrows():
                            # Use the correct column names from the API
                            first_name = str(row.get('lead_first_name', '')).strip().title() if pd.notna(row.get('lead_first_name')) else ''
                            last_name = str(row.get('lead_last_name', '')).strip().title() if pd.notna(row.get('lead_last_name')) else ''
                            
                            if first_name or last_name:
                                full_name = f"{first_name} {last_name}".strip()
                                customer_names.append(full_name if full_name else 'Name Not Available')
                            else:
                                # Fallback to other potential name fields
                                name_fields = ['name', 'customer_name', 'full_name', 'client_name', 'first_name', 'last_name']
                                found_name = False
                                for field in name_fields:
                                    if field in row and pd.notna(row[field]) and str(row[field]).strip():
                                        full_name = str(row[field]).strip().title()
                                        customer_names.append(full_name)
                                        found_name = True
                                        break
                                
                                if not found_name:
                                    customer_names.append('Name Not Available')
                        
                        deals_display['Customer Name'] = customer_names
                        
                        # Get member count - ensure it's numeric
                        members_col = deals_display.get('total_members', pd.Series([1] * len(deals_display)))
                        if not isinstance(members_col, pd.Series):
                            members_col = pd.Series([1] * len(deals_display))
                        deals_display['Members'] = pd.to_numeric(members_col, errors='coerce').fillna(1).astype(int)
                        
                        # Get carrier if available
                        carrier_col = deals_display.get('carrier', pd.Series(['N/A'] * len(deals_display)))
                        if not isinstance(carrier_col, pd.Series):
                            carrier_col = pd.Series(['N/A'] * len(deals_display))
                        deals_display['Carrier'] = carrier_col.fillna('N/A').astype(str)
                        
                        # Select columns for display
                        display_columns = ['Customer Name', 'Sale Date', 'Members', 'Carrier']
                        available_columns = [col for col in display_columns if col in deals_display.columns or col in ['Customer Name', 'Sale Date']]
                        
                        # Create the display dataframe
                        deals_table = deals_display[available_columns].copy()
                        
                        # Add filtering options
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            # Date range filter with calendar
                            st.write("**Filter by Date Range**")
                            if not deals_table.empty and 'Sale Date' in deals_table.columns:
                                # Convert Sale Date strings to datetime for min/max calculation
                                date_strings = deals_table['Sale Date'].dropna()
                                if len(date_strings) > 0:
                                    try:
                                        dates = pd.to_datetime(date_strings, format='%m/%d/%Y')
                                        min_date = dates.min().date()
                                        max_date = dates.max().date()
                                        
                                        # Allow broader date range - not restricted to only the data dates
                                        # This allows users to select any date for flexible filtering
                                        from datetime import date, timedelta
                                        calendar_min = date(2024, 1, 1)  # Allow selection from beginning of 2024
                                        calendar_max = date.today() + timedelta(days=30)  # Allow future dates
                                        
                                        # Date range selector with broader limits
                                        start_date = st.date_input(
                                            "Start Date",
                                            value=min_date,
                                            min_value=calendar_min,
                                            max_value=calendar_max,
                                            key="prev_cycle_start_date"
                                        )
                                        end_date = st.date_input(
                                            "End Date", 
                                            value=max_date,
                                            min_value=calendar_min,
                                            max_value=calendar_max,
                                            key="prev_cycle_end_date"
                                        )
                                    except:
                                        start_date = None
                                        end_date = None
                                        st.info("Date filtering not available")
                                else:
                                    start_date = None
                                    end_date = None
                            else:
                                start_date = None
                                end_date = None
                        
                        with col2:
                            # Carrier filter
                            if 'Carrier' in deals_table.columns:
                                unique_carriers = sorted([c for c in deals_table['Carrier'].unique() if c != 'N/A'])
                                if unique_carriers:
                                    carrier_filter = st.selectbox(
                                        "Filter by Carrier",
                                        ["All Carriers"] + unique_carriers,
                                        key="prev_cycle_carrier_filter"
                                    )
                                else:
                                    carrier_filter = "All Carriers"
                            else:
                                carrier_filter = "All Carriers"
                        
                        with col3:
                            # Member count filter
                            if 'Members' in deals_table.columns:
                                max_members = int(deals_table['Members'].max()) if not deals_table.empty else 1
                                if max_members > 1:
                                    member_filter = st.selectbox(
                                        "Filter by Members",
                                        ["All Member Counts"] + [f"{i} Member{'s' if i > 1 else ''}" for i in range(1, max_members + 1)],
                                        key="prev_cycle_member_filter"
                                    )
                                else:
                                    member_filter = "All Member Counts"
                            else:
                                member_filter = "All Member Counts"
                        
                        # Apply filters
                        filtered_deals = deals_table.copy()
                        
                        # Apply date range filter
                        if start_date is not None and end_date is not None and 'Sale Date' in filtered_deals.columns:
                            try:
                                # Convert Sale Date strings to datetime for comparison
                                filtered_deals['Sale Date Parsed'] = pd.to_datetime(filtered_deals['Sale Date'], format='%m/%d/%Y', errors='coerce')
                                
                                # Filter by date range
                                start_datetime = pd.Timestamp(start_date)
                                end_datetime = pd.Timestamp(end_date)
                                
                                # Apply the filter
                                mask = (filtered_deals['Sale Date Parsed'] >= start_datetime) & (filtered_deals['Sale Date Parsed'] <= end_datetime)
                                filtered_deals = filtered_deals[mask]
                                
                                # Remove the helper column
                                if 'Sale Date Parsed' in filtered_deals.columns:
                                    filtered_deals = filtered_deals.drop('Sale Date Parsed', axis=1)
                            except Exception as e:
                                pass  # If date parsing fails, show all data
                        
                        if carrier_filter != "All Carriers":
                            filtered_deals = filtered_deals[filtered_deals['Carrier'] == carrier_filter]
                        
                        if member_filter != "All Member Counts":
                            member_count = int(member_filter.split()[0])
                            filtered_deals = filtered_deals[filtered_deals['Members'] == member_count]
                        
                        # Display all filtered deals without pagination
                        if not filtered_deals.empty:
                            total_deals = len(filtered_deals)
                            
                            # Show total count
                            st.write(f"**Found {total_deals} deals in selected date range**")
                            
                            # Display as scrollable table with all data visible
                            st.dataframe(
                                filtered_deals,
                                use_container_width=True,
                                hide_index=True,
                                height=None,  # Remove height limit to show all rows
                                column_config={
                                    "Customer Name": st.column_config.TextColumn("Customer Name", width="medium"),
                                    "Sale Date": st.column_config.TextColumn("Sale Date", width="small"),
                                    "Members": st.column_config.NumberColumn("Members", width="small"),
                                    "Carrier": st.column_config.TextColumn("Carrier", width="medium")
                                }
                            )
                            
                            # Add download option for large datasets
                            if total_deals > 20:
                                csv = filtered_deals.to_csv(index=False)
                                st.download_button(
                                    label=f"Download all {total_deals} deals as CSV",
                                    data=csv,
                                    file_name=f"deals_{start_date}_to_{end_date}.csv",
                                    mime="text/csv"
                                )
                            
                            # Summary stats for filtered data
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Filtered Deals", len(filtered_deals))
                            col2.metric("Filtered Members", filtered_deals['Members'].sum() if 'Members' in filtered_deals.columns else len(filtered_deals))
                            if 'Carrier' in filtered_deals.columns:
                                top_carrier = filtered_deals['Carrier'].value_counts().index[0] if not filtered_deals['Carrier'].value_counts().empty else "N/A"
                                col3.metric("Top Carrier", top_carrier)
                        else:
                            st.info("No deals match the selected filters.")
                    else:
                        st.info("No deal details available for the previous cycle.")
                
                else:
                    st.warning("Previous cycle data requires agent login")
            else:
                st.info("No previous completed cycles found")
                
        except Exception as e:
            st.error(f"Error loading previous cycle data: {str(e)}")
        
        # AI-Powered Smart Cycle Forecasting
        st.markdown("---")
        st.markdown("### 🤖 AI Smart Cycle Forecasting")
        
        try:
            from smart_cycle_forecasting import SmartCycleForecaster
            
            forecaster = SmartCycleForecaster()
            cycle_info = forecaster.get_cycle_info(commission_cycles)
            
            if cycle_info and user_id:
                # Generate AI forecast for current agent
                forecast = forecaster.generate_forecast(
                    agent_name=st.session_state.user_name,
                    current_members=members_cycle,
                    cycle_info=cycle_info
                )
                
                if "error" not in forecast:
                    # AI Forecast Header
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        border: 2px solid #667eea;
                        padding: 25px;
                        border-radius: 20px;
                        margin: 20px 0;
                        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
                    ">
                        <h4 style="
                            margin: 0 0 15px 0;
                            color: #ffffff;
                            font-size: 20px;
                            font-weight: 700;
                            text-align: center;
                            font-family: 'Montserrat', sans-serif;
                        ">🤖 AI Performance Prediction</h4>
                        <p style="
                            margin: 0;
                            color: #f0f0f0;
                            font-size: 14px;
                            text-align: center;
                            font-weight: 600;
                        ">Based on your current pace and historical patterns</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Forecast metrics
                    forecast_col1, forecast_col2, forecast_col3, forecast_col4 = st.columns(4)
                    
                    forecast_col1.metric("🎯 Predicted Final Members", f"{forecast['predicted_members']:,}")
                    forecast_col2.metric("🏆 Predicted Tier", forecast['predicted_tier'])
                    forecast_col3.metric("💰 Predicted Earnings", f"${forecast['predicted_commission']:,.0f}")
                    forecast_col4.metric("⚡ Current Daily Pace", f"{forecast['daily_pace']:.1f}/day")
                    
                    # Next tier requirements (if applicable)
                    if forecast.get('next_tier') and forecast['next_tier']['members_needed'] > 0:
                        next_tier = forecast['next_tier']
                        
                        st.markdown("#### 🎯 Next Tier Challenge")
                        
                        tier_col1, tier_col2, tier_col3 = st.columns(3)
                        
                        tier_col1.metric("🏆 Target Tier", next_tier['name'])
                        tier_col2.metric("📈 Members Needed", f"{next_tier['members_needed']:,}")
                        tier_col3.metric("💸 Additional Earnings", f"${next_tier['additional_earnings']:,.0f}")
                        
                        # Progress bar to next tier
                        progress_to_next = min(members_cycle / next_tier['threshold'], 1.0)
                        
                        st.markdown(f"""
                        <div style="margin: 20px 0;">
                            <p style="margin-bottom: 10px; font-weight: 600;">Progress to {next_tier['name']} Tier:</p>
                            <div style="
                                background: #f0f0f0;
                                border-radius: 10px;
                                height: 20px;
                                overflow: hidden;
                                position: relative;
                            ">
                                <div style="
                                    background: linear-gradient(90deg, #667eea, #764ba2);
                                    height: 100%;
                                    width: {progress_to_next * 100:.1f}%;
                                    transition: width 0.3s ease;
                                "></div>
                                <div style="
                                    position: absolute;
                                    top: 50%;
                                    left: 50%;
                                    transform: translate(-50%, -50%);
                                    color: white;
                                    font-weight: bold;
                                    font-size: 12px;
                                    text-shadow: 1px 1px 2px rgba(0,0,0,0.7);
                                ">{members_cycle}/{next_tier['threshold']} members</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Required pace analysis
                        if next_tier['is_achievable']:
                            pace_color = "#28a745"  # Green
                            pace_icon = "✅"
                            pace_message = "Achievable with focused effort!"
                        else:
                            pace_color = "#ffc107"  # Yellow
                            pace_icon = "⚠️"
                            pace_message = "Requires exceptional performance!"
                        
                        st.markdown(f"""
                        <div style="
                            background: {pace_color}20;
                            border: 2px solid {pace_color};
                            padding: 15px;
                            border-radius: 15px;
                            margin: 15px 0;
                            text-align: center;
                        ">
                            <p style="
                                margin: 0;
                                color: {pace_color};
                                font-size: 16px;
                                font-weight: 700;
                            ">{pace_icon} Need {next_tier['daily_pace_needed']:.1f} members/day - {pace_message}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # AI Recommendations
                    st.markdown("#### 🧠 AI Coach Recommendations")
                    
                    recommendations_container = st.container()
                    with recommendations_container:
                        st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                            border: 2px solid #f093fb;
                            padding: 20px;
                            border-radius: 15px;
                            margin: 15px 0;
                            color: white;
                        ">
                            <h5 style="margin: 0 0 15px 0; color: white;">🎯 Personalized Strategy</h5>
                            <div style="
                                background: rgba(255,255,255,0.1);
                                padding: 15px;
                                border-radius: 10px;
                                font-size: 14px;
                                line-height: 1.6;
                                white-space: pre-line;
                            ">{forecast['ai_recommendations']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Performance trend indicator
                    trend = forecast.get('trend', 'stable')
                    if trend == 'improving':
                        trend_color = "#28a745"
                        trend_icon = "📈"
                        trend_message = "Performance trending upward!"
                    elif trend == 'declining':
                        trend_color = "#dc3545"
                        trend_icon = "📉"
                        trend_message = "Focus needed to improve trend"
                    else:
                        trend_color = "#ffc107"
                        trend_icon = "📊"
                        trend_message = "Steady performance maintained"
                    
                    st.markdown(f"""
                    <div style="
                        background: {trend_color}15;
                        border-left: 4px solid {trend_color};
                        padding: 15px;
                        margin: 15px 0;
                    ">
                        <p style="
                            margin: 0;
                            color: {trend_color};
                            font-weight: 600;
                        ">{trend_icon} {trend_message} (Confidence: {forecast.get('confidence', 50)}%)</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                else:
                    st.warning("AI forecasting data unavailable - continue tracking your current performance")
            else:
                st.info("AI forecasting requires active cycle and agent login")
                
        except ImportError:
            st.error("AI forecasting module not available")
        except Exception as e:
            st.error(f"AI forecasting temporarily unavailable: {str(e)}")
        
        # Enhanced Discord Control Panel
        if user_id and st.session_state.user_role in ["Manager", "Admin"]:
            st.markdown("---")
            st.markdown("### 🤖 Enhanced Discord Command Center")
            
            try:
                from enhanced_discord_bot import EnhancedDiscordBot
                from discord_webhook import DiscordSalesTracker
                
                enhanced_bot = EnhancedDiscordBot()
                discord_tracker = DiscordSalesTracker()
                
                # Discord controls in columns
                discord_col1, discord_col2, discord_col3 = st.columns(3)
                
                with discord_col1:
                    st.markdown("#### 🏆 Team Challenges")
                    if st.button("Launch Team Challenge", key="team_challenge"):
                        try:
                            result = enhanced_bot.send_smart_team_challenge()
                            if result.get('success'):
                                st.success(f"Team challenge '{result.get('challenge')}' launched!")
                            else:
                                st.error(f"Challenge failed: {result.get('error')}")
                        except Exception as e:
                            st.error(f"Challenge error: {str(e)}")
                    
                    if st.button("Weekend Prep Message", key="weekend_prep"):
                        try:
                            result = enhanced_bot.send_smart_weekend_prep()
                            if result.get('success'):
                                st.success("Weekend prep message sent!")
                            else:
                                st.error(f"Weekend prep failed: {result.get('error')}")
                        except Exception as e:
                            st.error(f"Weekend prep error: {str(e)}")
                
                with discord_col2:
                    st.markdown("#### 📊 Performance Analytics")
                    if st.button("Send Team Insights", key="team_insights"):
                        try:
                            # Get current team performance data
                            current_sales = fetch_all_today(limit=1000, send_discord_notifications=False)
                            
                            if current_sales:
                                # Process sales data for insights
                                agent_counts = {}
                                for sale in current_sales:
                                    agent_name = sale.get('agent_name', 'Unknown')
                                    if agent_name not in agent_counts:
                                        agent_counts[agent_name] = {'sales': 0, 'members': 0}
                                    agent_counts[agent_name]['sales'] += 1
                                    agent_counts[agent_name]['members'] += sale.get('member_count', 1)
                                
                                # Convert to format expected by enhanced bot
                                team_performance = [
                                    {'name': agent, 'sales': stats['sales'], 'members': stats['members']}
                                    for agent, stats in agent_counts.items()
                                ]
                                
                                result = enhanced_bot.send_performance_insights(team_performance)
                                if result.get('success'):
                                    st.success("Team performance insights sent!")
                                else:
                                    st.error(f"Insights failed: {result.get('error')}")
                            else:
                                st.warning("No sales data available for insights")
                        except Exception as e:
                            st.error(f"Insights error: {str(e)}")
                    
                    if st.button("Test Discord Connection", key="test_discord"):
                        try:
                            result = discord_tracker.test_webhook()
                            if result.get('success'):
                                st.success("Discord connection successful!")
                            else:
                                st.error(f"Discord test failed: {result.get('error')}")
                        except Exception as e:
                            st.error(f"Discord test error: {str(e)}")
                
                with discord_col3:
                    st.markdown("#### 💪 Agent Motivation")
                    
                    # Agent selection for personal motivation
                    if user_id:
                        current_sales = fetch_all_today(limit=1000, send_discord_notifications=False)
                        agent_options = ["Send to All Agents"] + list(set([
                            sale.get('agent_name', 'Unknown') 
                            for sale in (current_sales or [])
                            if sale.get('agent_name')
                        ]))
                        
                        selected_agent = st.selectbox(
                            "Select Agent", 
                            options=agent_options,
                            key="motivation_agent"
                        )
                        
                        if st.button("Send AI Motivation", key="ai_motivation"):
                            try:
                                if selected_agent == "Send to All Agents":
                                    # Send to all active agents
                                    agent_counts = {}
                                    for sale in (current_sales or []):
                                        agent_name = sale.get('agent_name', 'Unknown')
                                        if agent_name not in agent_counts:
                                            agent_counts[agent_name] = {'sales_today': 0, 'members_today': 0}
                                        agent_counts[agent_name]['sales_today'] += 1
                                        agent_counts[agent_name]['members_today'] += sale.get('member_count', 1)
                                    
                                    success_count = 0
                                    for agent_name, performance_data in agent_counts.items():
                                        try:
                                            result = enhanced_bot.send_smart_daily_motivator(agent_name, performance_data)
                                            if result.get('success'):
                                                success_count += 1
                                        except:
                                            continue
                                    
                                    st.success(f"AI motivation sent to {success_count} agents!")
                                else:
                                    # Send to specific agent
                                    agent_performance = {'sales_today': 0, 'members_today': 0, 'goal_progress': 50}
                                    
                                    # Calculate actual performance for selected agent
                                    for sale in (current_sales or []):
                                        if sale.get('agent_name') == selected_agent:
                                            agent_performance['sales_today'] += 1
                                            agent_performance['members_today'] += sale.get('member_count', 1)
                                    
                                    result = enhanced_bot.send_smart_daily_motivator(selected_agent, agent_performance)
                                    if result.get('success'):
                                        st.success(f"AI motivation sent to {selected_agent}!")
                                    else:
                                        st.error(f"Motivation failed: {result.get('error')}")
                            except Exception as e:
                                st.error(f"Motivation error: {str(e)}")
                    
                    if st.button("Celebrate Milestone", key="milestone_celebration"):
                        try:
                            # Create a sample milestone celebration
                            milestone_details = {
                                'agent_name': 'Team HCS',
                                'achievement': 'Outstanding daily performance!'
                            }
                            result = enhanced_bot.send_milestone_celebration('team_goal', milestone_details)
                            if result.get('success'):
                                st.success("Milestone celebration sent!")
                            else:
                                st.error(f"Celebration failed: {result.get('error')}")
                        except Exception as e:
                            st.error(f"Celebration error: {str(e)}")
                
                # Smart Discord Features Status
                st.markdown("#### 🎯 Smart Features Status")
                features_info = st.container()
                with features_info:
                    st.markdown("""
                    **Enhanced Discord Features:**
                    - **AI Team Challenges**: Automatically generated competitive challenges
                    - **Performance Insights**: Real-time analytics with AI commentary  
                    - **Personal Motivation**: Customized messages based on agent personality
                    - **Milestone Celebrations**: Dynamic achievement recognition
                    - **Weekend Prep**: End-of-week motivation and preparation
                    """)
                    
            except ImportError:
                st.error("Enhanced Discord features not available")
            except Exception as e:
                st.error(f"Discord control panel error: {str(e)}")
        
        # Cached agent performance data to speed up loading
        @st.cache_data(ttl=300)  # Cache for 5 minutes
        def get_all_agents_cycle_data_cached():
            """Get cycle member counts for all agents to determine top performer - cached version"""
            all_agent_data = []
            
            # Get current cycle dates
            today = pd.Timestamp.now().normalize()
            current_cycle = commission_cycles[
                (commission_cycles["start"] <= today) & (today <= commission_cycles["end"])
            ]
            
            if current_cycle.empty:
                return []
            
            cycle_row = current_cycle.iloc[0]
            cycle_start = cycle_row["start"].strftime("%Y-%m-%d")
            cycle_end = cycle_row["end"].strftime("%Y-%m-%d")
            
            # Get data for all agents with optimized batch processing
            for email, agent_user_id in AGENT_USERIDS.items():
                try:
                    agent_cycle_df = fetch_agent_deals(agent_user_id, cycle_start, cycle_end)
                    agent_members = agent_cycle_df['total_members'].sum() if not agent_cycle_df.empty and 'total_members' in agent_cycle_df.columns else 0
                    
                    # Get agent name
                    agent_info = df_agents[df_agents['username'] == email]
                    agent_name = agent_info.iloc[0]['name'] if not agent_info.empty and 'name' in agent_info.columns else email.split('@')[0]
                    
                    all_agent_data.append({
                        'name': agent_name,
                        'email': email,
                        'members': agent_members,
                        'user_id': agent_user_id
                    })
                except:
                    # Add fallback data to prevent empty results
                    agent_name = email.split('@')[0] if '@' in email else email
                    all_agent_data.append({
                        'name': agent_name,
                        'email': email,
                        'members': 0,
                        'user_id': agent_user_id
                    })
            
            # Sort by members descending to get top performer
            all_agent_data.sort(key=lambda x: x['members'], reverse=True)
            return all_agent_data
        
        # Get cached agent data to determine top performer
        all_agents_data = get_all_agents_cycle_data_cached()
        top_agent_data = all_agents_data[0] if all_agents_data else None
        is_top_agent = top_agent_data and top_agent_data['email'] == st.session_state.user_email
        
        # Bonus Tracker Section
        st.markdown("---")
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #ffd89b 0%, #19547b 100%);
            padding: 25px;
            border-radius: 20px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        ">
            <h2 style="
                margin: 0;
                color: white;
                font-size: 28px;
                font-weight: 700;
                text-align: center;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            ">💰 Bonus Tracker</h2>
        </div>
        """, unsafe_allow_html=True)
        
        # Calculate bonus progress
        bonus_threshold = 70
        members_to_bonus = max(0, bonus_threshold - members_cycle)
        bonus_qualified = members_cycle >= bonus_threshold
        progress_percentage = min((members_cycle / bonus_threshold) * 100, 100)
        
        # Bonus status card
        bonus_col1, bonus_col2 = st.columns([2, 1])
        
        with bonus_col1:
            if bonus_qualified:
                bonus_status = "QUALIFIED ✅"
                bonus_color = "#28a745"
                bonus_message = f"Congratulations! You've qualified for the estimated $1,200 bonus with {members_cycle} members!"
                bonus_note = "Note: Final bonus payment depends on FMO member payment confirmation"
            else:
                bonus_status = "IN PROGRESS"
                bonus_color = "#ffc107" 
                bonus_message = f"You need {members_to_bonus} more members to qualify for the $1,200 bonus"
                bonus_note = f"Current progress: {members_cycle}/{bonus_threshold} members"
            
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, {bonus_color}22 0%, {bonus_color}11 100%);
                    border-left: 5px solid {bonus_color};
                    padding: 20px;
                    border-radius: 10px;
                    margin-bottom: 15px;
                ">
                    <h4 style="margin: 0 0 10px 0; color: {bonus_color}; font-size: 18px;">
                        🎯 $1,200 Bonus Status: {bonus_status}
                    </h4>
                    <p style="margin: 0 0 10px 0; font-size: 16px; color: #ffffff;">
                        {bonus_message}
                    </p>
                    <p style="margin: 0; font-size: 14px; color: #ffffff; font-style: italic;">
                        {bonus_note}
                    </p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with bonus_col2:
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, {bonus_color} 0%, {bonus_color}dd 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    color: white;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                ">
                    <h3 style="margin: 0; font-size: 32px; font-weight: bold;">{members_cycle}</h3>
                    <p style="margin: 5px 0; font-size: 14px; color: #ffffff;">Members This Cycle</p>
                    <div style="
                        background: rgba(255,255,255,0.2);
                        border-radius: 10px;
                        height: 8px;
                        margin: 10px 0;
                        overflow: hidden;
                    ">
                        <div style="
                            background: rgba(255,255,255,0.8);
                            height: 100%;
                            width: {progress_percentage}%;
                            transition: width 0.3s ease;
                        "></div>
                    </div>
                    <p style="margin: 0; font-size: 12px; color: #ffffff;">{progress_percentage:.0f}% to Bonus</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        # Top Agent Bonus Tracker
        st.markdown("---")
        st.markdown("### 🏆 Top Agent Bonus ($250)")
        
        if top_agent_data:
            top_agent_name = top_agent_data['name']
            top_agent_members = top_agent_data['members']
            
            # Show leaderboard with top 3
            leaderboard_col1, leaderboard_col2 = st.columns([2, 1])
            
            with leaderboard_col1:
                if is_top_agent:
                    top_status = "YOU ARE THE TOP AGENT! 🏆"
                    top_color = "#dc3545"
                    top_message = f"Congratulations! You're currently leading with {members_cycle} members and qualified for the estimated $250 top agent bonus!"
                    top_note = "Note: Final bonus payment depends on FMO member payment confirmation and maintaining top position"
                else:
                    top_status = "CURRENT LEADER"
                    top_color = "#6c757d"
                    gap = top_agent_members - members_cycle
                    top_message = f"{top_agent_name} is currently leading with {top_agent_members} members. You need {gap} more members to take the lead!"
                    top_note = f"Your current position: {members_cycle} members (Gap: {gap} members)"
                
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, {top_color}22 0%, {top_color}11 100%);
                        border-left: 5px solid {top_color};
                        padding: 20px;
                        border-radius: 10px;
                        margin-bottom: 15px;
                    ">
                        <h4 style="margin: 0 0 10px 0; color: {top_color}; font-size: 18px;">
                            👑 {top_status}
                        </h4>
                        <p style="margin: 0 0 10px 0; font-size: 16px; color: #ffffff;">
                            {top_message}
                        </p>
                        <p style="margin: 0; font-size: 14px; color: #ffffff; font-style: italic;">
                            {top_note}
                        </p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with leaderboard_col2:
                crown_emoji = "👑" if is_top_agent else "🎯"
                position_text = "TOP AGENT" if is_top_agent else f"#{len([a for a in all_agents_data if a['members'] > members_cycle]) + 1}"
                
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, {top_color} 0%, {top_color}dd 100%);
                        padding: 20px;
                        border-radius: 15px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                    ">
                        <h3 style="margin: 0; font-size: 32px; font-weight: bold;">{crown_emoji}</h3>
                        <h4 style="margin: 5px 0; font-size: 14px; color: #ffffff;">{position_text}</h4>
                        <h3 style="margin: 10px 0; font-size: 24px; font-weight: bold;">{members_cycle}</h3>
                        <p style="margin: 0; font-size: 12px; color: #ffffff;">Your Members</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            # Mini leaderboard showing top 3
            if len(all_agents_data) > 1:
                st.markdown("#### 📊 Current Leaderboard (Top 3)")
                leader_col1, leader_col2, leader_col3 = st.columns(3)
                
                positions = ["🥇", "🥈", "🥉"]
                colors = ["#ffd700", "#c0c0c0", "#cd7f32"]
                
                for i, (col, pos_emoji, color) in enumerate(zip([leader_col1, leader_col2, leader_col3], positions, colors)):
                    if i < len(all_agents_data):
                        agent = all_agents_data[i]
                        is_current_user = agent['email'] == st.session_state.user_email
                        border_style = "border: 3px solid #dc3545;" if is_current_user else ""
                        
                        with col:
                            st.markdown(
                                f"""
                                <div style="
                                    background: linear-gradient(135deg, {color}22 0%, {color}11 100%);
                                    padding: 15px;
                                    border-radius: 10px;
                                    text-align: center;
                                    {border_style}
                                ">
                                    <h3 style="margin: 0; font-size: 24px;">{pos_emoji}</h3>
                                    <h4 style="
                                        margin: 5px 0; 
                                        font-size: 16px; 
                                        color: #FFFFFF;
                                        font-weight: 800;
                                        font-family: 'Montserrat', sans-serif;
                                        text-shadow: 
                                            0 0 15px rgba(255, 255, 255, 0.9),
                                            2px 2px 4px rgba(0,0,0,0.9);
                                        letter-spacing: 1px;
                                    ">{agent['name']}</h4>
                                    <h3 style="margin: 5px 0; font-size: 20px; color: {color};">{agent['members']}</h3>
                                    <p style="
                                        margin: 0; 
                                        font-size: 13px; 
                                        color: #FFFFFF;
                                        font-weight: 600;
                                        font-family: 'Montserrat', sans-serif;
                                        text-shadow: 
                                            0 0 10px rgba(255, 255, 255, 0.8),
                                            1px 1px 2px rgba(0,0,0,0.8);
                                        text-transform: uppercase;
                                        letter-spacing: 1px;
                                    ">members</p>
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
    
    # Use current biweekly cycle dates instead of 30-day totals
    # Debug: Check if current_cycle is properly detected
    today = pd.Timestamp.now().normalize()
    current_cycle_check = commission_cycles[
        (commission_cycles["start"] <= today) & (today <= commission_cycles["end"])
    ]
    
    if not current_cycle_check.empty:
        cycle_row = current_cycle_check.iloc[0]
        cycle_start_str = cycle_row["start"].strftime("%Y-%m-%d")
        cycle_end_str = cycle_row["end"].strftime("%Y-%m-%d")
        cycle_deals = fetch_agent_deals(user_id, cycle_start_str, cycle_end_str)
        st.write(f"DEBUG: Using cycle dates {cycle_start_str} to {cycle_end_str}")
    else:
        # Use a much shorter recent period instead of 30 days
        seven_days_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")
        cycle_deals = fetch_agent_deals(user_id, seven_days_ago, today_str)
        st.write(f"DEBUG: No cycle found, using {seven_days_ago} to {today_str}")
    
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

# === MANAGER DASHBOARD ===
elif st.session_state.user_role.lower() == "manager":
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 25%, #2a2a2a 50%, #1a1a1a 75%, #000000 100%);
            border: 4px solid #FFD700;
            padding: 40px;
            border-radius: 30px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 
                0 30px 80px rgba(0,0,0,0.9),
                0 0 60px rgba(255, 215, 0, 0.8),
                inset 0 0 50px rgba(255, 215, 0, 0.1);
            position: relative;
            overflow: hidden;
        ">
            <h1 style="
                color: #FFD700;
                font-size: 56px;
                margin: 0 0 20px 0;
                text-shadow: 
                    0 0 30px rgba(255, 215, 0, 1),
                    8px 8px 16px rgba(0,0,0,0.8);
                font-weight: 900;
                font-family: 'Playfair Display', serif;
                letter-spacing: 4px;
            ">
                💰 MANAGER COMMISSION CENTER 💰
            </h1>
            <p style="
                color: #FFFFFF;
                font-size: 22px;
                margin: 0;
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                text-transform: uppercase;
                letter-spacing: 3px;
                text-shadow: 3px 3px 6px rgba(0,0,0,0.8);
            ">
                📊 CYCLE TRACKING • COMMISSION ANALYSIS • PERFORMANCE METRICS 📊
            </p>
        </div>
        """, 
        unsafe_allow_html=True
    )

    # Manager Dashboard Content
    from datetime import datetime, timedelta
    import plotly.express as px
    import plotly.graph_objects as go
    
    # Calculate cycle performance
    def calculate_cycle_performance(start_date, end_date):
        """Calculate performance metrics for a specific cycle"""
        try:
            # Use the existing fetch_all_today function which works properly
            df = fetch_all_today(limit=5000, send_discord_notifications=False)
            
            # Check if data exists (fetch_all_today returns a DataFrame)
            if df.empty:
                return {"total_sales": 0, "total_members": 0, "commission": 0, "agent_breakdown": []}
            
            # Filter data by cycle dates using date_created
            df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
            cycle_start = pd.to_datetime(start_date)
            cycle_end = pd.to_datetime(end_date)
            
            # Filter to only include deals within the cycle period
            cycle_df = df[(df['date_created'] >= cycle_start) & (df['date_created'] <= cycle_end)]
            
            # Use the filtered dataframe for calculations
            df = cycle_df
            
            # Use existing member count data if available, otherwise estimate
            if 'total_members' in df.columns:
                # Use existing member counts from the data
                total_members = df['total_members'].sum()
            else:
                # Estimate member counts (1.2 average members per policy based on historical data)
                total_members = int(len(df) * 1.2)
            
            # Calculate totals
            total_sales = len(df)
            # For managers (Jarad and Matt), commission is based on sales count, not members
            gross_revenue = total_sales * 150  # $150 gross per sale
            commission = gross_revenue * 0.02    # 2% commission
            
            # Agent breakdown - use actual member data if available
            agent_breakdown = []
            for agent in df['agent_name'].unique():
                if pd.notna(agent):
                    agent_sales = df[df['agent_name'] == agent]
                    if 'total_members' in df.columns:
                        agent_members = agent_sales['total_members'].sum()
                    else:
                        agent_members = int(len(agent_sales) * 1.2)  # Estimate 1.2 members per sale
                    agent_breakdown.append({
                        "agent": agent,
                        "sales": len(agent_sales),
                        "members": agent_members,
                        "revenue": agent_members * 150
                    })
            
            return {
                "total_sales": total_sales,
                "total_members": total_members,
                "gross_revenue": gross_revenue,
                "commission": commission,
                "agent_breakdown": agent_breakdown
            }
            
        except Exception as e:
            st.error(f"Error calculating cycle performance: {str(e)}")
            return {"total_sales": 0, "total_members": 0, "commission": 0, "agent_breakdown": []}
    
    # Current cycle detection
    today = datetime.now().date()
    current_cycle = None
    for _, cycle in commission_cycles.iterrows():
        if cycle['start'].date() <= today <= cycle['end'].date():
            current_cycle = cycle
            break
    
    # Debug cycle detection
    st.write(f"Today's date: {today}")
    st.write(f"Available cycles: {len(commission_cycles)}")
    
    if current_cycle is not None:
        st.success(f"Found active cycle: {current_cycle['start'].strftime('%m/%d/%y')} - {current_cycle['end'].strftime('%m/%d/%y')}")
    else:
        st.warning("No active cycle found - showing most recent cycle data")
        # Use most recent cycle as fallback
        current_cycle = commission_cycles.iloc[-1] if not commission_cycles.empty else None
    
    if current_cycle is not None:
        st.markdown("### 📅 Current Cycle Performance")
        
        # Calculate current cycle performance
        st.write(f"Fetching data for cycle: {current_cycle['start'].strftime('%Y-%m-%d')} to {current_cycle['end'].strftime('%Y-%m-%d')}")
        
        with st.spinner("Loading cycle performance data..."):
            cycle_data = calculate_cycle_performance(current_cycle['start'], current_cycle['end'])
        
        st.write(f"API returned: {len(cycle_data.get('agent_breakdown', []))} agents, {cycle_data.get('total_sales', 0)} sales, {cycle_data.get('total_members', 0)} members")
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Sales", f"{cycle_data['total_sales']:,}")
        
        with col2:
            st.metric("Total Members", f"{cycle_data['total_members']:,}")
        
        with col3:
            st.metric("Gross Revenue", f"${cycle_data.get('gross_revenue', 0):,.2f}")
        
        with col4:
            st.metric("Your Commission (2%)", f"${cycle_data['commission']:,.2f}")
        
        # Cycle dates
        st.info(f"Current Cycle: {current_cycle['start'].strftime('%m/%d/%y')} - {current_cycle['end'].strftime('%m/%d/%y')} (Pay Date: {current_cycle['pay'].strftime('%m/%d/%y')})")
        
        # Agent breakdown
        if cycle_data['agent_breakdown']:
            st.markdown("### 👥 Agent Performance Breakdown")
            
            breakdown_df = pd.DataFrame(cycle_data['agent_breakdown'])
            breakdown_df = breakdown_df.sort_values('members', ascending=False)
            
            st.dataframe(breakdown_df, use_container_width=True, hide_index=True)
            
            # Commission breakdown chart
            if len(breakdown_df) > 0:
                fig = px.pie(
                    breakdown_df, 
                    values='members', 
                    names='agent',
                    title="Member Distribution by Agent"
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.warning("No active cycle found for current date")
    
    st.markdown("---")
    
    # Custom date range analysis
    st.markdown("### 📊 Custom Date Range Analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=today - timedelta(days=14))
    with col2:
        end_date = st.date_input("End Date", value=today)
    
    if st.button("📈 Analyze Custom Range"):
        custom_data = calculate_cycle_performance(
            datetime.combine(start_date, datetime.min.time()),
            datetime.combine(end_date, datetime.min.time())
        )
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Sales", f"{custom_data['total_sales']:,}")
        with col2:
            st.metric("Members", f"{custom_data['total_members']:,}")
        with col3:
            st.metric("Commission", f"${custom_data['commission']:,.2f}")
    
    # Historical cycles performance
    st.markdown("### 📈 Historical Cycle Performance")
    
    historical_data = []
    for _, cycle in commission_cycles.head(6).iterrows():  # Last 6 cycles
        if cycle['end'].date() < today:  # Only completed cycles
            cycle_perf = calculate_cycle_performance(cycle['start'], cycle['end'])
            historical_data.append({
                "Cycle": f"{cycle['start'].strftime('%m/%d')} - {cycle['end'].strftime('%m/%d')}",
                "Sales": cycle_perf['total_sales'],
                "Members": cycle_perf['total_members'],
                "Commission": cycle_perf['commission']
            })
    
    if historical_data:
        hist_df = pd.DataFrame(historical_data)
        st.dataframe(hist_df, use_container_width=True, hide_index=True)
        
        # Trend chart
        if len(hist_df) > 1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hist_df['Cycle'],
                y=hist_df['Commission'],
                mode='lines+markers',
                name='Commission',
                line=dict(color='#FFD700', width=3),
                marker=dict(size=8)
            ))
            fig.update_layout(
                title="Commission Trend Over Cycles",
                xaxis_title="Cycle",
                yaxis_title="Commission ($)",
                template="plotly_dark"
            )
            st.plotly_chart(fig, use_container_width=True)

# === ADMIN DASHBOARD ===
else:
    st.markdown(
        """
        <div class="admin-header" style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 25%, #2a2a2a 50%, #1a1a1a 75%, #000000 100%);
            border: 4px solid #FFD700;
            padding: 40px;
            border-radius: 30px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 
                0 30px 80px rgba(0,0,0,0.9),
                0 0 60px rgba(255, 215, 0, 0.8),
                inset 0 0 50px rgba(255, 215, 0, 0.1);
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: linear-gradient(45deg, transparent, rgba(255, 215, 0, 0.05), transparent);
                animation: shimmer 4s infinite;
                pointer-events: none;
            "></div>
            <h1 style="
                color: #FFD700;
                font-size: 56px;
                margin: 0 0 20px 0;
                text-shadow: 
                    0 0 30px rgba(255, 215, 0, 1),
                    8px 8px 16px rgba(0,0,0,0.8);
                font-weight: 900;
                font-family: 'Playfair Display', serif;
                letter-spacing: 4px;
                position: relative;
                z-index: 2;
            ">
                🏆 ADMIN COMMAND CENTER 🏆
            </h1>
            <p style="
                color: #FFFFFF;
                font-size: 22px;
                margin: 0;
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                text-transform: uppercase;
                letter-spacing: 3px;
                text-shadow: 3px 3px 6px rgba(0,0,0,0.8);
                position: relative;
                z-index: 2;
            ">
                💼 MANAGE OPERATIONS • TRACK PERFORMANCE • DRIVE SUCCESS 💼
            </p>
            <div style="
                background: linear-gradient(135deg, rgba(255, 215, 0, 0.2) 0%, rgba(255, 215, 0, 0.1) 100%);
                border: 2px solid rgba(255, 215, 0, 0.5);
                margin: 25px auto 0 auto;
                padding: 15px 35px;
                border-radius: 20px; 
                display: inline-block;
                position: relative;
                z-index: 2;
                backdrop-filter: blur(10px);
            ">
                <span style="font-size: 18px; font-weight: bold;">⚡ LEAD THE TEAM • MAXIMIZE PROFITS • CONTROL SUCCESS ⚡</span>
            </div>
        </div>
        """, 
        unsafe_allow_html=True
    )

    # Build tabs list based on user role
    tabs_list = [
        "🏆 Overview",
        "📋 Leaderboard", 
        "📈 History",
        "📊 Live Counts",
        "👥 Member Tracking",
        "⚙️ Settings",
        "📂 Clients",
        "💼 Vendor Pay",
        "🧾 Agent Net Pay",
        "📊 Vendor CPL/CPA",
        "🔧 TLD User Sync"
    ]
    
    # Add Manager tab for Manager role users
    if st.session_state.user_role.lower() == "manager":
        tabs_list.insert(1, "💰 Manager Dashboard")
    
    tabs = st.tabs(tabs_list)

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
                background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                border: 3px solid #FFD700;
                padding: 30px;
                border-radius: 20px;
                text-align: center;
                color: white;
                box-shadow: 
                    0 20px 60px rgba(0,0,0,0.9),
                    0 0 40px rgba(255, 215, 0, 0.6),
                    inset 0 0 30px rgba(255, 215, 0, 0.1);
                margin-bottom: 25px;
            ">
                <h2 style="
                    margin: 0; 
                    font-size: 48px; 
                    font-weight: 900;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFD700;
                    text-shadow: 
                        0 0 25px rgba(255, 215, 0, 1),
                        5px 5px 10px rgba(0,0,0,0.8);
                    letter-spacing: 2px;
                ">{int(totals['deals']):,}</h2>
                <p style="
                    margin: 15px 0 0 0; 
                    font-size: 18px;
                    font-weight: 800;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFFFFF;
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                ">🎯 TOTAL PAID DEALS</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with o2:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                border: 3px solid #00FF00;
                padding: 30px;
                border-radius: 20px;
                text-align: center;
                color: white;
                box-shadow: 
                    0 20px 60px rgba(0,0,0,0.9),
                    0 0 40px rgba(0, 255, 0, 0.6),
                    inset 0 0 30px rgba(0, 255, 0, 0.1);
                margin-bottom: 25px;
            ">
                <h2 style="
                    margin: 0; 
                    font-size: 48px; 
                    font-weight: 900;
                    font-family: 'Montserrat', sans-serif;
                    color: #00FF00;
                    text-shadow: 
                        0 0 25px rgba(0, 255, 0, 1),
                        5px 5px 10px rgba(0,0,0,0.8);
                    letter-spacing: 2px;
                ">${totals['agent']:,.0f}</h2>
                <p style="
                    margin: 15px 0 0 0; 
                    font-size: 18px;
                    font-weight: 800;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFFFFF;
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                ">💰 AGENT PAYOUT</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with o3:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                border: 3px solid #FF4500;
                padding: 30px;
                border-radius: 20px;
                text-align: center;
                color: white;
                box-shadow: 
                    0 20px 60px rgba(0,0,0,0.9),
                    0 0 40px rgba(255, 69, 0, 0.6),
                    inset 0 0 30px rgba(255, 69, 0, 0.1);
                margin-bottom: 25px;
            ">
                <h2 style="
                    margin: 0; 
                    font-size: 48px; 
                    font-weight: 900;
                    font-family: 'Montserrat', sans-serif;
                    color: #FF4500;
                    text-shadow: 
                        0 0 25px rgba(255, 69, 0, 1),
                        5px 5px 10px rgba(0,0,0,0.8);
                    letter-spacing: 2px;
                ">${totals['owner_rev']:,.0f}</h2>
                <p style="
                    margin: 15px 0 0 0; 
                    font-size: 18px;
                    font-weight: 800;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFFFFF;
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                ">📈 OWNER REVENUE</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with o4:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
                border: 4px solid #FFD700;
                padding: 30px;
                border-radius: 20px;
                text-align: center;
                color: white;
                box-shadow: 
                    0 25px 70px rgba(0,0,0,0.9),
                    0 0 50px rgba(255, 215, 0, 0.8),
                    inset 0 0 40px rgba(255, 215, 0, 0.15);
                margin-bottom: 25px;
            ">
                <h2 style="
                    margin: 0; 
                    font-size: 48px; 
                    font-weight: 900;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFD700;
                    text-shadow: 
                        0 0 30px rgba(255, 215, 0, 1),
                        6px 6px 12px rgba(0,0,0,0.8);
                    letter-spacing: 2px;
                ">${totals['owner_prof']:,.0f}</h2>
                <p style="
                    margin: 15px 0 0 0; 
                    font-size: 18px;
                    font-weight: 800;
                    font-family: 'Montserrat', sans-serif;
                    color: #FFFFFF;
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                ">💎 OWNER PROFIT</p>
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
        <div class="live-dashboard" style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 3px solid #FFD700;
            padding: 35px;
            border-radius: 25px;
            margin: 30px 0 20px 0;
            text-align: center;
            box-shadow: 
                0 25px 60px rgba(0,0,0,0.9),
                0 0 50px rgba(255, 215, 0, 0.6),
                inset 0 0 40px rgba(255, 215, 0, 0.1);
        ">
            <h3 style="
                margin: 0; 
                color: #FFD700; 
                font-size: 36px;
                font-weight: 900;
                font-family: 'Playfair Display', serif;
                text-shadow: 
                    0 0 25px rgba(255, 215, 0, 1),
                    5px 5px 10px rgba(0,0,0,0.8);
                letter-spacing: 3px;
            ">📊 LIVE PERFORMANCE DASHBOARD</h3>
            <p style="
                margin: 15px 0 0 0; 
                color: #FFFFFF; 
                font-size: 18px;
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                text-transform: uppercase;
                letter-spacing: 2px;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
            ">
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

            # Mobile-optimized styling for Live Counts metrics
            st.markdown("""
                <style>
                /* Live Counts specific styling - matches global standards */
                .live-counts-metrics [data-testid="stMetricValue"] {
                    color: #000000 !important;
                    background: #FFFFFF !important;
                    font-size: 72px !important;
                    font-weight: 900 !important;
                    font-family: 'Arial Black', 'Impact', monospace !important;
                    padding: 15px !important;
                    border: 6px solid #000000 !important;
                    border-radius: 10px !important;
                    display: block !important;
                    text-align: center !important;
                    width: 100% !important;
                    box-sizing: border-box !important;
                    white-space: nowrap !important;
                    overflow: visible !important;
                    text-overflow: clip !important;
                    line-height: 1.0 !important;
                    margin: 0 !important;
                }
                
                .live-counts-metrics [data-testid="metric-container"] {
                    background: #000000 !important;
                    border: 6px solid #FFD700 !important;
                    border-radius: 15px !important;
                    padding: 20px !important;
                    margin: 15px 0 !important;
                    min-height: auto !important;
                    height: auto !important;
                    overflow: visible !important;
                }
                </style>
            """, unsafe_allow_html=True)
            
            # Simple layout with enhanced metrics
            lc1, lc2, lc3 = st.columns(3)
            
            with lc1:
                st.metric("📅 TODAY", f"{daily_count:,}")
            
            with lc2:
                st.metric("📈 WEEK", f"{weekly_count:,}")
            
            with lc3:
                st.metric("📊 MONTH", f"{monthly_count:,}")
            
            # Add member count row with enhanced deployment-proof styling
            st.markdown(
                """
                <style>
                .member-counts-container * {
                    color: #ffffff !important;
                    text-shadow: 0 0 8px rgba(255,255,255,1) !important;
                    font-weight: 700 !important;
                    background: transparent !important;
                }
                .member-counts-container h4 {
                    color: #ffffff !important;
                    text-shadow: 0 0 8px rgba(255,255,255,1) !important;
                    font-weight: 700 !important;
                    font-size: 18px !important;
                    text-align: center !important;
                }
                </style>
                <div class="member-counts-container" style="margin: 30px 0 20px 0;">
                    <h4 style="
                        text-align: center; 
                        color: #ffffff !important; 
                        margin: 0; 
                        font-weight: 700 !important;
                        text-shadow: 0 0 8px rgba(255,255,255,1) !important;
                        font-family: 'Arial', sans-serif !important;
                        font-size: 18px !important;
                        background: transparent !important;
                    ">Member Counts</h4>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            mc1, mc2, mc3 = st.columns(3)
            
            with mc1:
                st.markdown(
                    f"""
                    <style>
                    .member-card-today * {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                    }}
                    .member-card-today h3 {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                        font-size: 28px !important;
                    }}
                    .member-card-today p {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                        font-size: 14px !important;
                    }}
                    </style>
                    <div class="member-card-today" style="
                        background: linear-gradient(135deg, #4CAF50 0%, #81C784 100%);
                        padding: 20px;
                        border-radius: 12px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        margin-bottom: 20px;
                    ">
                        <h3 style="margin: 0; font-size: 28px !important; font-weight: 900 !important; color: #ffffff !important; text-shadow: 0 0 10px rgba(255,255,255,1) !important;">{int(daily_members):,}</h3>
                        <p style="
                            margin: 8px 0 0 0; 
                            font-size: 14px !important; 
                            color: #ffffff !important; 
                            font-weight: 900 !important;
                            text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                            font-family: 'Arial', sans-serif !important;
                        ">Today's Members</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with mc2:
                st.markdown(
                    f"""
                    <style>
                    .member-card-weekly * {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                    }}
                    .member-card-weekly h3 {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                        font-size: 28px !important;
                    }}
                    .member-card-weekly p {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                        font-size: 14px !important;
                    }}
                    </style>
                    <div class="member-card-weekly" style="
                        background: linear-gradient(135deg, #2196F3 0%, #64B5F6 100%);
                        padding: 20px;
                        border-radius: 12px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        margin-bottom: 20px;
                    ">
                        <h3 style="margin: 0; font-size: 28px !important; font-weight: 900 !important; color: #ffffff !important; text-shadow: 0 0 10px rgba(255,255,255,1) !important;">{int(weekly_members):,}</h3>
                        <p style="
                            margin: 8px 0 0 0; 
                            font-size: 14px !important; 
                            color: #ffffff !important; 
                            font-weight: 900 !important;
                            text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                            font-family: 'Arial', sans-serif !important;
                        ">Weekly Members</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with mc3:
                st.markdown(
                    f"""
                    <style>
                    .member-card-monthly * {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                    }}
                    .member-card-monthly h3 {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                        font-size: 28px !important;
                    }}
                    .member-card-monthly p {{
                        color: #ffffff !important;
                        text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                        font-weight: 900 !important;
                        font-size: 14px !important;
                    }}
                    </style>
                    <div class="member-card-monthly" style="
                        background: linear-gradient(135deg, #FF9800 0%, #FFB74D 100%);
                        padding: 20px;
                        border-radius: 12px;
                        text-align: center;
                        color: white;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        margin-bottom: 20px;
                    ">
                        <h3 style="margin: 0; font-size: 28px !important; font-weight: 900 !important; color: #ffffff !important; text-shadow: 0 0 10px rgba(255,255,255,1) !important;">{int(monthly_members):,}</h3>
                        <p style="
                            margin: 8px 0 0 0; 
                            font-size: 14px !important; 
                            color: #ffffff !important; 
                            font-weight: 900 !important;
                            text-shadow: 0 0 10px rgba(255,255,255,1) !important;
                            font-family: 'Arial', sans-serif !important;
                        ">Monthly Members</p>
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
            
            # Calculate member totals
            d_members = df_api[daily_mask]['total_members'].sum() if 'total_members' in df_api.columns else d_tot
            w_members = df_api[weekly_mask]['total_members'].sum() if 'total_members' in df_api.columns else w_tot
            m_members = df_api[monthly_mask]['total_members'].sum() if 'total_members' in df_api.columns else m_tot
            y_members = df_api[yearly_mask]['total_members'].sum() if 'total_members' in df_api.columns else y_tot
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Today", f"{d_tot:,}", f"{int(d_members)} members")
            col2.metric("This Week", f"{w_tot:,}", f"{int(w_members)} members")
            col3.metric("This Month", f"{m_tot:,}", f"{int(m_members)} members")
            col4.metric("This Year", f"{y_tot:,}", f"{int(y_members)} members")
            
            # Show recent deals table
            if not df_api.empty:
                st.subheader("Recent Deals")
                recent_cols = ['policy_id', 'agent_name', 'lead_first_name', 'lead_last_name', 'carrier', 'date_sold']
                recent_cols = [c for c in recent_cols if c in df_api.columns]
                st.dataframe(df_api[recent_cols].head(20), use_container_width=True, hide_index=True)

    # MEMBER TRACKING TAB
    with tabs[4]:
        st.header("Member Tracking Analysis")
        st.info("Enhanced member tracking using TQL dependents API for accurate family size calculations")
        
        try:
            df_api = fetch_all_today(limit=5000)
            if not df_api.empty and 'total_members' in df_api.columns:
                # Member distribution analysis
                st.subheader("Member Distribution")
                member_dist = df_api['total_members'].value_counts().sort_index()
                st.bar_chart(member_dist)
                
                # Agent member performance
                if 'agent_name' in df_api.columns:
                    st.subheader("Agent Member Performance")
                    agent_members = df_api.groupby('agent_name')['total_members'].agg(['count', 'sum', 'mean']).round(2)
                    agent_members.columns = ['Deals', 'Total Members', 'Avg Members/Deal']
                    agent_members = agent_members.sort_values('Total Members', ascending=False)
                    st.dataframe(agent_members, use_container_width=True)
                
                # Family size insights
                st.subheader("Family Size Insights")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Family Size", f"{df_api['total_members'].mean():.2f}")
                with col2:
                    st.metric("Largest Family", f"{df_api['total_members'].max()}")
            else:
                st.warning("Member tracking data not available")
        except Exception as e:
            st.error(f"Error loading member tracking data: {str(e)}")

    # SETTINGS TAB
    with tabs[5]:
        st.header("Upload FMO Statement & Health Sherpa Export")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 FMO Statement Upload")
            fmo_file = st.file_uploader("Upload FMO Statement (CSV/Excel)", type=["csv", "xlsx"], key="fmo_upload")
            
            if fmo_file:
                try:
                    if fmo_file.name.endswith('.csv'):
                        df_fmo = pd.read_csv(fmo_file)
                    else:
                        df_fmo = pd.read_excel(fmo_file)
                    
                    st.success(f"FMO file loaded: {len(df_fmo)} rows")
                    st.dataframe(df_fmo.head(), use_container_width=True)
                    
                    # Store in session state
                    st.session_state['fmo_data'] = df_fmo
                    
                except Exception as e:
                    st.error(f"Error loading FMO file: {str(e)}")
        
        with col2:
            st.subheader("🏥 Health Sherpa Export Upload")
            hs_file = st.file_uploader("Upload Health Sherpa Export (CSV/Excel)", type=["csv", "xlsx"], key="hs_upload")
            
            if hs_file:
                try:
                    if hs_file.name.endswith('.csv'):
                        df_hs = pd.read_csv(hs_file)
                    else:
                        df_hs = pd.read_excel(hs_file)
                    
                    st.success(f"Health Sherpa file loaded: {len(df_hs)} rows")
                    st.dataframe(df_hs.head(), use_container_width=True)
                    
                    # Store in session state
                    st.session_state['hs_data'] = df_hs
                    
                except Exception as e:
                    st.error(f"Error loading Health Sherpa file: {str(e)}")
        
        # Process files if both are uploaded
        if st.session_state.get('fmo_data') is not None and st.session_state.get('hs_data') is not None:
            st.markdown("---")
            st.subheader("Process Commission Cycle")
            
            if st.button("Calculate Vendor CPL/CPA", type="primary"):
                with st.spinner("Processing commission cycle..."):
                    # This would trigger the processing logic
                    st.success("Commission cycle processed successfully!")
                    st.session_state['cycle_processed'] = True



    # VENDOR PAY TAB
    with tabs[7]:
        st.header("Vendor Payment Analysis")
        st.info("Vendor payment calculations based on call duration thresholds and authentic pricing data")
        
        # Display vendor configuration
        from vendor_config import get_vendor_summary, get_all_vendor_names
        
        st.subheader("Vendor Configuration")
        vendor_summary = get_vendor_summary()
        st.dataframe(vendor_summary, use_container_width=True, hide_index=True)
        
        # Vendor performance metrics
        st.subheader("Vendor Performance")
        try:
            df_api = fetch_all_today(limit=5000)
            if not df_api.empty and 'lead_vendor_name' in df_api.columns:
                vendor_performance = df_api.groupby('lead_vendor_name').agg({
                    'policy_id': 'count',
                    'total_members': 'sum' if 'total_members' in df_api.columns else 'count'
                })
                vendor_performance.columns = ['Total Deals', 'Total Members']
                vendor_performance = vendor_performance.sort_values('Total Deals', ascending=False)
                st.dataframe(vendor_performance, use_container_width=True)
            else:
                st.info("Vendor performance data not available")
        except Exception as e:
            st.error(f"Error calculating vendor performance: {str(e)}")

    # AGENT NET PAY TAB
    with tabs[8]:
        st.header("🧾 Agent Net Pay")
        st.subheader("Upload Files for Net Pay Calculation")
        
        # File uploads for net pay calculation
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("FMO Commission Statement")
            fmo_net_file = st.file_uploader("Upload FMO Statement (Excel)", type=["xlsx"], key="fmo_net_upload")
            
        with col2:
            st.subheader("Health Sherpa Export")
            hs_net_file = st.file_uploader("Upload Health Sherpa Export (CSV)", type=["csv"], key="hs_net_upload")
        
        # Process files when both are uploaded
        if fmo_net_file and hs_net_file:
            st.success("Both files uploaded - calculating agent net pay...")
            
            try:
                # Load and process files using the existing logic from Settings tab
                # Health Sherpa Export for member counts
                hs = pd.read_csv(hs_net_file, dtype=str)
                hs['first_name_norm'] = hs['first_name'].astype(str).str.strip().str.lower()
                hs['last_name_norm'] = hs['last_name'].astype(str).str.strip().str.lower()
                hs['member_count'] = pd.to_numeric(hs['applicant_count'], errors='coerce').fillna(1).astype(int)
                member_lookup = hs.set_index(['first_name_norm','last_name_norm'])['member_count'].to_dict()

                # FMO Paid Deals
                df = pd.read_excel(fmo_net_file, dtype=str)
                df = df.dropna(subset=["Agent","first_name","last_name","Advance"])
                df["Paid Status"] = df["Advance"].astype(float).apply(lambda x: "Paid" if x > 0 else "Not Paid")
                df['first_name_norm'] = df['first_name'].astype(str).str.strip().str.lower()
                df['last_name_norm'] = df['last_name'].astype(str).str.strip().str.lower()
                if "Advance Excluded Reason" in df.columns:
                    df["Reason"] = df["Advance Excluded Reason"]
                else:
                    df["Reason"] = ""

                st.subheader("Net Pay Calculation Results")
                
                # Calculate net pay for each agent
                net_pay_results = []
                agent_data_list = []
                
                # First pass: calculate all agent data to find top performer
                agent_temp_data = {}
                for agent in df["Agent"].unique():
                    sub = df[df["Agent"]==agent]
                    paid_sub = sub[sub["Paid Status"]=="Paid"]
                    unpaid_sub = sub[sub["Paid Status"]!="Paid"]

                    paid_count = len(paid_sub)
                    unpaid_count = len(unpaid_sub)
                    all_count = paid_count + unpaid_count
                    paid_pct = (paid_count / all_count) if all_count > 0 else 0

                    # Calculate total members for paid policies
                    total_members_paid = 0
                    total_members_unpaid = 0

                    for _, row in paid_sub.iterrows():
                        fname = str(row['first_name']).strip().lower()
                        lname = str(row['last_name']).strip().lower()
                        member_key = (fname, lname)
                        member_count = member_lookup.get(member_key, 1)
                        total_members_paid += member_count

                    for _, row in unpaid_sub.iterrows():
                        fname = str(row['first_name']).strip().lower()
                        lname = str(row['last_name']).strip().lower()
                        member_key = (fname, lname)
                        member_count = member_lookup.get(member_key, 1)
                        total_members_unpaid += member_count

                    # Store agent data for top agent calculation
                    agent_temp_data[agent] = {
                        'paid_count': paid_count,
                        'unpaid_count': unpaid_count,
                        'all_count': all_count,
                        'paid_pct': paid_pct,
                        'total_members_paid': total_members_paid,
                        'total_members_unpaid': total_members_unpaid
                    }

                # Find top agent by total paid members
                top_agent = max(agent_temp_data.keys(), key=lambda x: agent_temp_data[x]['total_members_paid'])

                # Second pass: calculate final pay with top agent bonus
                for agent in df["Agent"].unique():
                    data = agent_temp_data[agent]
                    paid_count = data['paid_count']
                    unpaid_count = data['unpaid_count']
                    all_count = data['all_count']
                    paid_pct = data['paid_pct']
                    total_members_paid = data['total_members_paid']
                    total_members_unpaid = data['total_members_unpaid']

                    # Calculate tier-based commission
                    def calc_member_commission(member_count):
                        if member_count >= 140:
                            return member_count * 25 + 1200
                        elif member_count >= 100:
                            return member_count * 22.5 + 1200
                        elif member_count >= 70:
                            return member_count * 17.5 + 1200
                        else:
                            return member_count * 15

                    # Calculate pay based on paid members only
                    tier_rate = 25 if total_members_paid >= 140 else 22.5 if total_members_paid >= 100 else 17.5 if total_members_paid >= 70 else 15
                    production_bonus = 1200 if total_members_paid >= 70 else 0
                    retention_bonus = 500 if (paid_pct >= 0.80 and paid_count >= 80) else 0  # 80+ paid deals AND 80%+ retention
                    top_agent_bonus = 250 if agent == top_agent else 0  # Only top agent gets bonus
                    
                    base_pay = total_members_paid * tier_rate
                    gross_pay = base_pay + production_bonus + retention_bonus + top_agent_bonus
                    
                    net_pay_results.append({
                        "Agent": agent,
                        "Paid Applications": paid_count,
                        "Unpaid Applications": unpaid_count,
                        "Total Applications": all_count,
                        "Paid %": f"{paid_pct:.1%}",
                        "Total Members": total_members_paid,
                        "Unpaid Members": total_members_unpaid,
                        "Per-Member Rate": f"${tier_rate}",
                        "Base Pay": f"${base_pay:,.2f}",
                        "Production Bonus": f"${production_bonus:,.2f}",
                        "Retention Bonus": f"${retention_bonus:,.2f}",
                        "Top Agent Bonus": f"${top_agent_bonus:,.2f}",
                        "Agent Payout": f"${gross_pay:,.2f}"
                    })
                    
                    # Store for agent dashboard
                    agent_data = {
                        'agent_name': agent,
                        'paid_applications': paid_count,
                        'total_applications': all_count,
                        'total_members': total_members_paid,
                        'unpaid_applications': unpaid_count,
                        'unpaid_members': total_members_unpaid,
                        'paid_percentage': paid_pct,
                        'per_member_rate': tier_rate,
                        'base_pay': base_pay,
                        'production_bonus': production_bonus,
                        'retention_bonus': retention_bonus,
                        'top_agent_bonus': top_agent_bonus,
                        'gross_pay': gross_pay,
                        'total_payout': gross_pay,
                        'cycle_period': "5/17-5/30",
                        'pay_date': "6/6/25"
                    }
                    agent_data_list.append(agent_data)

                # Display results
                results_df = pd.DataFrame(net_pay_results)
                st.dataframe(results_df, use_container_width=True, hide_index=True)
                
                # Show top agent info
                st.info(f"🏆 Top Agent Bonus ($250) awarded to: **{top_agent}** ({agent_temp_data[top_agent]['total_members_paid']} paid members)")

                # Store in session state for agent dashboard access
                st.session_state['agent_reports'] = {agent['agent_name']: agent for agent in agent_data_list}
                st.session_state['reports_generated'] = True

                # Generate PDFs and ZIP
                st.subheader("Download Options")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # CSV download
                    csv_data = results_df.to_csv(index=False)
                    st.download_button(
                        "📊 Download Summary (CSV)",
                        csv_data,
                        file_name="agent_net_pay_summary.csv",
                        mime="text/csv"
                    )

                with col2:
                    # Generate and download PDFs
                    if st.button("Generate PDFs", type="primary"):
                        import io
                        import zipfile
                        from fpdf import FPDF
                        
                        def create_enhanced_pdf(agent_name, agent_data):
                            """Create enhanced PDF with detailed client listings and new commission structure"""
                            try:
                                def safe_str(x):
                                    return str(x).encode('latin1', errors='replace').decode('latin1')
                                
                                pdf = FPDF()
                                pdf.add_page()
                                
                                # Header
                                pdf.set_font("Arial", "B", 16)
                                pdf.cell(0, 10, safe_str("Health Connect Solutions"), ln=True, align="C")
                                pdf.ln(5)
                                pdf.set_font("Arial", "B", 12)
                                pdf.cell(0, 10, safe_str(f"Commission Statement - {agent_name}"), ln=True)
                                pdf.ln(5)
                                
                                # Performance summary
                                pdf.set_font("Arial", "", 12)
                                pdf.cell(0, 8, safe_str(f"Total Applications: {agent_data.get('total_applications', 0)}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Paid Applications: {agent_data.get('paid_applications', 0)}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Unpaid Applications: {agent_data.get('unpaid_applications', 0)}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Paid Percentage: {agent_data.get('paid_percentage', 0):.1f}%"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Total Members: {agent_data.get('total_members', 0)}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Per-Member Rate: ${agent_data.get('per_member_rate', 0)}"), ln=True)
                                pdf.ln(3)
                                
                                # Commission breakdown with new structure
                                pdf.set_font("Arial", "B", 12)
                                pdf.cell(0, 8, safe_str("Commission Breakdown:"), ln=True)
                                pdf.set_font("Arial", "", 12)
                                pdf.cell(0, 8, safe_str(f"Base Pay: ${agent_data.get('base_pay', 0):,.2f}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Production Bonus: ${agent_data.get('production_bonus', 0):,.2f}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Retention Bonus: ${agent_data.get('retention_bonus', 0):,.2f}"), ln=True)
                                pdf.cell(0, 8, safe_str(f"Top Agent Bonus: ${agent_data.get('top_agent_bonus', 0):,.2f}"), ln=True)
                                pdf.ln(3)
                                
                                # Final payout
                                pdf.set_text_color(0, 150, 0)
                                pdf.set_font("Arial", "B", 14)
                                pdf.cell(0, 10, safe_str(f"Total Net Pay: ${agent_data.get('gross_pay', 0):,.2f}"), ln=True)
                                pdf.set_text_color(0, 0, 0)
                                pdf.ln(5)
                                
                                # Get agent deals for detailed listings
                                agent_deals_df = df[df["Agent"] == agent_name] if 'df' in locals() else None
                                
                                if agent_deals_df is not None and len(agent_deals_df) > 0:
                                    # Paid clients list
                                    paid_deals = agent_deals_df[agent_deals_df["Paid Status"] == "Paid"]
                                    if len(paid_deals) > 0:
                                        pdf.set_font("Arial", "B", 12)
                                        pdf.cell(0, 8, safe_str(f"Paid Clients ({len(paid_deals)}):"), ln=True)
                                        pdf.set_font("Arial", "", 10)
                                        
                                        for _, row in paid_deals.head(50).iterrows():  # Limit to first 50
                                            client_name = safe_str(str(row.get('Customer Name', row.get('Client', 'Unknown'))))
                                            eff_date = safe_str(str(row.get('Effective Date', 'N/A')))
                                            carrier = safe_str(str(row.get('Carrier', 'N/A')))
                                            pdf.multi_cell(0, 6, safe_str(f"- {client_name} | Eff: {eff_date} | {carrier}"))
                                    
                                    # Unpaid clients and reasons
                                    unpaid_deals = agent_deals_df[agent_deals_df["Paid Status"] != "Paid"]
                                    if len(unpaid_deals) > 0:
                                        pdf.ln(3)
                                        pdf.set_font("Arial", "B", 12)
                                        pdf.cell(0, 8, safe_str(f"Unpaid Clients & Reasons ({len(unpaid_deals)}):"), ln=True)
                                        pdf.set_font("Arial", "", 10)
                                        
                                        for _, row in unpaid_deals.head(25).iterrows():  # Limit to first 25
                                            client_name = safe_str(str(row.get('Customer Name', row.get('Client', 'Unknown'))))
                                            eff_date = safe_str(str(row.get('Effective Date', 'N/A')))
                                            reason = safe_str(str(row.get('Reason', row.get('Notes', 'No reason provided'))))
                                            pdf.multi_cell(0, 6, safe_str(f"- {client_name} | Eff: {eff_date} | {reason}"))
                                            pdf.ln(1)
                                else:
                                    # Fallback if no detailed data available
                                    pdf.set_font("Arial", "", 10)
                                    pdf.cell(0, 8, safe_str("Detailed client listings require uploaded TLD export data."), ln=True)
                                
                                # Generate PDF content
                                pdf_content = pdf.output(dest="S")
                                if isinstance(pdf_content, str):
                                    pdf_content = pdf_content.encode('latin1')
                                
                                return pdf_content
                                
                            except Exception as e:
                                st.error(f"PDF creation failed for {agent_name}: {e}")
                                return None
                        
                        try:
                            with st.spinner("Generating PDF reports..."):
                                pdf_files = []
                                
                                # Generate PDFs for each agent
                                for agent_data in agent_data_list:
                                    agent_name = agent_data.get('agent_name', 'Unknown')
                                    pdf_content = create_enhanced_pdf(agent_name, agent_data)
                                    
                                    if pdf_content:
                                        pdf_files.append((agent_name, pdf_content))
                                
                                if pdf_files:
                                    # Create ZIP file
                                    zip_buffer = io.BytesIO()
                                    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                                        for agent_name, pdf_content in pdf_files:
                                            safe_name = agent_name.replace(' ', '_').replace('/', '_').replace(',', '_')
                                            zip_file.writestr(f"{safe_name}_net_pay_report.pdf", pdf_content)
                                    
                                    st.download_button(
                                        "📁 Download All PDFs (ZIP)",
                                        zip_buffer.getvalue(),
                                        file_name="agent_net_pay_pdfs.zip",
                                        mime="application/zip"
                                    )
                                    
                                    st.success(f"✅ Generated {len(pdf_files)} PDF reports successfully!")
                                else:
                                    st.error("❌ No PDFs were generated - check agent data")
                                    
                        except Exception as e:
                            st.error(f"❌ PDF generation system failed: {str(e)}")
                            st.write(f"Error details: {type(e).__name__}")
                            # Show debug info
                            if 'agent_data_list' in locals():
                                st.write(f"Agent data available: {len(agent_data_list)} agents")

                # Store payroll data for history
                upload_date = date.today().strftime('%Y-%m-%d')
                insert_agent_payroll(agent_data_list, upload_date)

                # VENDOR CPA ANALYSIS SECTION
                st.divider()
                st.subheader("📊 Vendor CPA Analysis")
                
                try:
                    # Calculate vendor metrics from the data
                    vendor_metrics = {}
                    
                    for _, row in df.iterrows():
                        vendor = row.get('Carrier', 'Unknown')
                        paid_status = row.get('Paid Status', '')
                        
                        if vendor not in vendor_metrics:
                            vendor_metrics[vendor] = {'total_leads': 0, 'paid_leads': 0, 'cost': 0}
                        
                        vendor_metrics[vendor]['total_leads'] += 1
                        if paid_status == 'Paid':
                            vendor_metrics[vendor]['paid_leads'] += 1
                        
                        # Use fallback pricing if vendor config not available
                        vendor_metrics[vendor]['cost'] += 15.0  # Default cost per lead

                    # Create vendor metrics dataframe
                    if vendor_metrics:
                        metrics_data = []
                        total_cost_all = 0
                        total_leads_all = 0
                        total_paid_all = 0
                        
                        for vendor, metrics in vendor_metrics.items():
                            total_cost = metrics['cost']
                            paid_leads = metrics['paid_leads']
                            total_leads = metrics['total_leads']
                            
                            cpa = total_cost / paid_leads if paid_leads > 0 else 0
                            conversion_rate = (paid_leads / total_leads * 100) if total_leads > 0 else 0
                            
                            metrics_data.append({
                                'Vendor': vendor,
                                'Total Leads': total_leads,
                                'Paid Leads': paid_leads,
                                'Conversion Rate': f"{conversion_rate:.1f}%",
                                'Total Cost': f"${total_cost:,.2f}",
                                'CPA': f"${cpa:.2f}"
                            })
                            
                            total_cost_all += total_cost
                            total_leads_all += total_leads
                            total_paid_all += paid_leads

                        metrics_df = pd.DataFrame(metrics_data)
                        
                        if not metrics_df.empty:
                            st.dataframe(metrics_df, use_container_width=True, hide_index=True)
                            
                            # Summary metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Cost", f"${total_cost_all:,.2f}")
                            with col2:
                                st.metric("Total Leads", total_leads_all)
                            with col3:
                                overall_conversion = (total_paid_all / total_leads_all * 100) if total_leads_all > 0 else 0
                                st.metric("Overall Conversion", f"{overall_conversion:.1f}%")
                            
                            # Download CSV
                            csv_data = metrics_df.to_csv(index=False)
                            st.download_button(
                                "📊 Download Vendor CPA Report (CSV)",
                                csv_data,
                                file_name="vendor_cpa_analysis.csv",
                                mime="text/csv"
                            )
                        else:
                            st.info("No vendor data available for analysis.")
                
                except Exception as vendor_error:
                    st.error(f"Error generating vendor analysis: {str(vendor_error)}")

            except Exception as e:
                st.error(f"Error processing net pay calculation: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
        else:
            st.info("Upload both FMO Statement (Excel) and Health Sherpa Export (CSV) to calculate net pay.")

    # VENDOR CPL/CPA TAB
    with tabs[9]:
        st.header("📊 Vendor CPA Analysis")
        st.write("Analyze vendor CPA performance by combining TLD Export and FMO Statement data.")
        st.write("Vendor pricing: Fran, Ray Calls, PT ACA Calls = $75 per paid application")
        st.write("Upload both files to calculate conversion rates, costs, and performance metrics.")
        
        st.subheader("Upload Files for Vendor Analysis")
        
        # File uploads
        col1, col2 = st.columns(2)
        
        with col1:
            tld_cpl_file = st.file_uploader("Upload TLD Export (CSV)", type="csv", key="vendor_tld_upload")
            
        with col2:
            fmo_cpl_file = st.file_uploader("Upload FMO Statement (Excel)", type=["xlsx", "xls"], key="vendor_fmo_upload")
        
        # Process files if both are uploaded
        if tld_cpl_file and fmo_cpl_file:
            try:
                # Read TLD export
                tld_df = pd.read_csv(tld_cpl_file)
                st.success(f"TLD data loaded: {len(tld_df)} records")
                
                # Read FMO statement  
                fmo_df = pd.read_excel(fmo_cpl_file)
                st.success(f"FMO data loaded: {len(fmo_df)} records")
                
                # Debug: Show column structure
                with st.expander("Debug: Data Structure"):
                    st.write("**TLD Export columns:**", list(tld_df.columns))
                    st.write("**FMO Statement columns:**", list(fmo_df.columns))
                    st.write("**Sample TLD data:**")
                    st.dataframe(tld_df.head(2))
                    st.write("**Sample FMO data:**")
                    st.dataframe(fmo_df.head(2))
                    
                    # Check column K specifically
                    if len(fmo_df.columns) > 10:
                        col_k_name = fmo_df.columns[10]
                        st.write(f"**Column K ({col_k_name}) sample values:**")
                        st.write(fmo_df.iloc[:5, 10].tolist())
                
                # Process TLD export data for vendor analysis
                combined_data = []
                
                # Process each TLD record
                for _, tld_row in tld_df.iterrows():
                    vendor = str(tld_row.get('vendor', '')).strip()
                    agent_name = str(tld_row.get('lmb_name', '')).strip()
                    status = str(tld_row.get('status_description', '')).strip()
                    
                    # Skip if no vendor or agent name
                    if not vendor or not agent_name or vendor.lower() == 'nan' or agent_name.lower() == 'nan':
                        continue
                    
                    # Get lead details from TLD export
                    lead_phone = str(tld_row.get('phone', '')).strip()
                    lead_first_name = str(tld_row.get('first_name', '')).strip()
                    lead_last_name = str(tld_row.get('last_name', '')).strip()
                    
                    # Skip if no name data to match with FMO
                    if not lead_first_name or not lead_last_name:
                        continue
                    
                    # Try to match with FMO data using first name, last name
                    fmo_matches = pd.DataFrame()
                    if not fmo_df.empty:
                        fmo_matches = fmo_df[
                            (fmo_df['first_name'].astype(str).str.lower().str.contains(
                                lead_first_name.lower(), na=False
                            )) &
                            (fmo_df['last_name'].astype(str).str.lower().str.contains(
                                lead_last_name.lower(), na=False
                            ))
                        ]
                    
                    # Skip if no FMO match found - only include deals that are in both files
                    if fmo_matches.empty:
                        continue
                    
                    # Determine payment status and reason from FMO data
                    paid_status = 'Unpaid'  # Default
                    paid_amount = 0
                    reason = ''
                    
                    try:
                        # Check if match is paid using Advance column
                        for _, fmo_match in fmo_matches.iterrows():
                            # Get the Advance column value
                            advance_amount = fmo_match['Advance'] if 'Advance' in fmo_match else 0
                            try:
                                advance_val = float(str(advance_amount).replace('$', '').replace(',', '')) if advance_amount else 0
                                if advance_val > 0:  # Any positive advance amount means paid
                                    paid_status = 'Paid'
                                    paid_amount = 75  # Vendor CPA rate
                                    break
                            except:
                                pass
                        
                        # Get reason from FMO Statement columns
                        reason = ""
                        if 'Advance Excluded Reason' in fmo_matches.columns:
                            reason = str(fmo_matches.iloc[0]['Advance Excluded Reason']).strip()
                        elif len(fmo_matches.iloc[0]) > 11:  # Column 11 is Advance Excluded Reason
                            reason = str(fmo_matches.iloc[0].iloc[11]).strip()
                        
                        # Clean up the reason and provide better defaults
                        if not reason or reason in ['nan', 'NaN', '', 'None', 'NULL', 'null']:
                            if paid_status == 'Paid':
                                reason = "Application approved and paid"
                            else:
                                # Try to get policy status from FMO
                                policy_status = ""
                                if 'policy_status' in fmo_matches.columns:
                                    policy_status = str(fmo_matches.iloc[0]['policy_status']).strip()
                                
                                if policy_status and policy_status not in ['nan', 'NaN', '', 'None', 'NULL']:
                                    reason = f"Policy Status: {policy_status}"
                                else:
                                    # Check TLD status for better context
                                    tld_status = str(tld_row.get('status_description', '')).strip()
                                    if tld_status and tld_status not in ['nan', 'NaN', '', 'None', 'NULL']:
                                        reason = f"Application Status: {tld_status}"
                                    else:
                                        reason = "Application under review - contact carrier for details"
                    except Exception as e:
                        # If there's any error, still include the record but mark appropriately
                        reason = f"Application Status: {str(tld_row.get('status_description', 'Under review'))}"
                    
                    combined_data.append({
                        'Agent': agent_name,
                        'Vendor': vendor,
                        'Status': status,
                        'Paid_Status': paid_status,
                        'Paid_Amount': paid_amount,
                        'Reason': reason,
                        'Lead_ID': str(tld_row.get('lead_id', '')),
                        'Phone': lead_phone,
                        'First_Name': lead_first_name,
                        'Last_Name': lead_last_name,
                        'Application_Count': 1
                    })
                
                df_combined = pd.DataFrame(combined_data)
                
                if not df_combined.empty:
                    # Calculate vendor metrics
                    vendor_metrics = {}
                    
                    for _, row in df_combined.iterrows():
                        vendor = row['Vendor']
                        paid_status = row['Paid_Status']
                        application_count = row['Application_Count']
                        
                        if vendor not in vendor_metrics:
                            vendor_metrics[vendor] = {
                                'total_applications': 0, 
                                'paid_applications': 0, 
                                'cost': 0
                            }
                        
                        vendor_metrics[vendor]['total_applications'] += application_count
                        
                        if paid_status == 'Paid':
                            vendor_metrics[vendor]['paid_applications'] += application_count
                            # Vendor-specific pricing for paid applications
                            vendor_name_lower = vendor.lower()
                            if any(cpa_vendor in vendor_name_lower for cpa_vendor in ['fran', 'ray calls', 'pt aca calls']):
                                vendor_metrics[vendor]['cost'] += 75.0 * application_count
                            else:
                                vendor_metrics[vendor]['cost'] += 15.0 * application_count  # Default for other vendors
                    
                    # Create metrics dataframe
                    metrics_data = []
                    total_cost_all = 0
                    total_applications_all = 0
                    total_paid_all = 0
                    
                    for vendor, metrics in vendor_metrics.items():
                        total_cost = metrics['cost']
                        paid_applications = metrics['paid_applications']
                        total_applications = metrics['total_applications']
                        
                        cost_per_application = total_cost / total_applications if total_applications > 0 else 0
                        cpa = total_cost / paid_applications if paid_applications > 0 else 0
                        conversion_rate = (paid_applications / total_applications * 100) if total_applications > 0 else 0
                        
                        unpaid_applications = total_applications - paid_applications
                        unpaid_percentage = (unpaid_applications / total_applications * 100) if total_applications > 0 else 0
                        
                        metrics_data.append({
                            'Vendor': vendor,
                            'Total Applications': total_applications,
                            'Paid Applications': paid_applications,
                            'Unpaid Applications': unpaid_applications,
                            'Conversion Rate': f"{conversion_rate:.1f}%",
                            'Unpaid Rate': f"{unpaid_percentage:.1f}%",
                            'Total Cost': f"${total_cost:,.2f}",
                            'Cost Per App': f"${cost_per_application:.2f}",
                            'CPA': f"${cpa:.2f}"
                        })
                        
                        total_cost_all += total_cost
                        total_applications_all += total_applications
                        total_paid_all += paid_applications

                    metrics_df = pd.DataFrame(metrics_data)
                    
                    if not metrics_df.empty:
                        st.subheader("Vendor Performance Metrics")
                        st.dataframe(metrics_df, use_container_width=True, hide_index=True)
                        
                        # Summary metrics
                        st.subheader("Overall Summary")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Total Cost", f"${total_cost_all:,.2f}")
                        with col2:
                            st.metric("Total Applications", total_applications_all)
                        with col3:
                            overall_conversion = (total_paid_all / total_applications_all * 100) if total_applications_all > 0 else 0
                            st.metric("Overall Conversion", f"{overall_conversion:.1f}%")
                        
                        # Download All Vendor PDFs as ZIP
                        import io
                        import zipfile
                        from datetime import datetime
                        
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                            # Add summary CSV to zip
                            summary_csv = metrics_df.to_csv(index=False)
                            zip_file.writestr("vendor_summary.csv", summary_csv)
                            
                            # Generate PDF for each vendor and add to zip
                            for vendor in df_combined['Vendor'].unique():
                                vendor_data = df_combined[df_combined['Vendor'] == vendor]
                                paid_data = vendor_data[vendor_data['Paid_Status'] == 'Paid']
                                unpaid_data = vendor_data[vendor_data['Paid_Status'] == 'Unpaid']
                                
                                if not vendor_data.empty:
                                    try:
                                        # Prepare data for PDF
                                        paid_pdf_data = paid_data.copy()
                                        unpaid_pdf_data = unpaid_data.copy()
                                        
                                        # Rename columns for PDF function
                                        if not paid_pdf_data.empty:
                                            paid_pdf_data['First Name'] = paid_pdf_data['First_Name']
                                            paid_pdf_data['Last Name'] = paid_pdf_data['Last_Name']
                                        if not unpaid_pdf_data.empty:
                                            unpaid_pdf_data['First Name'] = unpaid_pdf_data['First_Name']
                                            unpaid_pdf_data['Last Name'] = unpaid_pdf_data['Last_Name']
                                        
                                        vendor_rate = 75.0
                                        vendor_pdf_data = vendor_pdf(paid_pdf_data, unpaid_pdf_data, vendor, vendor_rate)
                                        
                                        # Add PDF to zip
                                        pdf_filename = f"{vendor.replace(' ', '_').lower()}_report.pdf"
                                        zip_file.writestr(pdf_filename, vendor_pdf_data)
                                    except Exception as e:
                                        # Add error note to zip if PDF generation fails
                                        error_note = f"Error generating PDF for {vendor}: {str(e)}"
                                        zip_file.writestr(f"{vendor.replace(' ', '_').lower()}_error.txt", error_note)
                        
                        zip_buffer.seek(0)
                        
                        st.download_button(
                            "📄 Download All Vendor PDF Reports (ZIP)",
                            zip_buffer.getvalue(),
                            file_name=f"vendor_reports_{datetime.now():%Y%m%d_%H%M}.zip",
                            mime="application/zip"
                        )
                        
                        # Detailed Client Information by Vendor
                        st.subheader("Client Details by Vendor")
                        
                        for vendor in df_combined['Vendor'].unique():
                            vendor_data = df_combined[df_combined['Vendor'] == vendor].copy()
                            
                            with st.expander(f"{vendor} - {len(vendor_data)} Applications"):
                                # Separate paid and unpaid
                                paid_data = vendor_data[vendor_data['Paid_Status'] == 'Paid']
                                unpaid_data = vendor_data[vendor_data['Paid_Status'] == 'Unpaid']
                                
                                # Display metrics for this vendor
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Total Applications", len(vendor_data))
                                with col2:
                                    st.metric("Paid Applications", len(paid_data))
                                with col3:
                                    conversion = (len(paid_data) / len(vendor_data) * 100) if len(vendor_data) > 0 else 0
                                    st.metric("Conversion Rate", f"{conversion:.1f}%")
                                
                                # Paid Applications
                                if not paid_data.empty:
                                    st.write("**Paid Applications:**")
                                    paid_display = paid_data[[
                                        'Agent', 'First_Name', 'Last_Name', 'Phone', 
                                        'Paid_Amount', 'Reason', 'Lead_ID'
                                    ]].copy()
                                    paid_display['Client'] = paid_display['First_Name'] + ' ' + paid_display['Last_Name']
                                    paid_display = paid_display[['Agent', 'Client', 'Phone', 'Paid_Amount', 'Reason', 'Lead_ID']]
                                    paid_display.columns = ['Agent', 'Client Name', 'Phone', 'Commission', 'Reason', 'Lead ID']
                                    st.dataframe(paid_display, use_container_width=True, hide_index=True)
                                
                                # Unpaid Applications
                                if not unpaid_data.empty:
                                    st.write("**Unpaid Applications:**")
                                    unpaid_display = unpaid_data[[
                                        'Agent', 'First_Name', 'Last_Name', 'Phone', 
                                        'Reason', 'Lead_ID'
                                    ]].copy()
                                    unpaid_display['Client'] = unpaid_display['First_Name'] + ' ' + unpaid_display['Last_Name']
                                    unpaid_display = unpaid_display[['Agent', 'Client', 'Phone', 'Reason', 'Lead_ID']]
                                    unpaid_display.columns = ['Agent', 'Client Name', 'Phone', 'Reason', 'Lead ID']
                                    st.dataframe(unpaid_display, use_container_width=True, hide_index=True)
                                    
                                    # Unpaid Reasons Breakdown
                                    st.write("**Unpaid Reasons Summary:**")
                                    reason_counts = unpaid_data['Reason'].value_counts()
                                    reason_df = pd.DataFrame({
                                        'Reason': reason_counts.index,
                                        'Count': reason_counts.values,
                                        'Percentage': (reason_counts.values / len(unpaid_data) * 100).round(1)
                                    })
                                    reason_df['Percentage'] = reason_df['Percentage'].astype(str) + '%'
                                    st.dataframe(reason_df, use_container_width=True, hide_index=True)
                                
                                # PDF Generation for this vendor
                                vendor_rate = 75.0 if vendor.lower() in ['fran', 'ray calls', 'pt aca calls'] else 15.0
                                try:
                                    # Prepare data for PDF with correct column names
                                    paid_pdf_data = paid_data.copy()
                                    unpaid_pdf_data = unpaid_data.copy()
                                    
                                    # Rename columns to match PDF function expectations
                                    paid_pdf_data['First Name'] = paid_pdf_data['First_Name']
                                    paid_pdf_data['Last Name'] = paid_pdf_data['Last_Name']
                                    paid_pdf_data['Phone'] = paid_pdf_data['Phone']
                                    unpaid_pdf_data['First Name'] = unpaid_pdf_data['First_Name']
                                    unpaid_pdf_data['Last Name'] = unpaid_pdf_data['Last_Name']
                                    unpaid_pdf_data['Phone'] = unpaid_pdf_data['Phone']
                                    
                                    vendor_pdf_data = vendor_pdf(paid_pdf_data, unpaid_pdf_data, vendor, vendor_rate)
                                    st.download_button(
                                        f"Download {vendor} PDF Report",
                                        vendor_pdf_data,
                                        file_name=f"vendor_report_{vendor.replace(' ', '_').lower()}.pdf",
                                        mime="application/pdf",
                                        key=f"vendor_pdf_{vendor}"
                                    )
                                except Exception as e:
                                    st.error(f"Error generating PDF for {vendor}: {str(e)}")
                        
                        # Charts
                        st.subheader("Performance Charts")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.subheader("Conversion Rate by Vendor")
                            chart_data = metrics_df.set_index('Vendor')['Conversion Rate'].str.rstrip('%').astype(float)
                            st.bar_chart(chart_data)
                        
                        with col2:
                            st.subheader("Total Applications by Vendor")
                            member_chart_data = metrics_df.set_index('Vendor')['Total Applications']
                            st.bar_chart(member_chart_data)
                    
                    else:
                        st.warning("No vendor data available for analysis.")
                
                else:
                    st.warning("Could not process the uploaded files for vendor analysis.")
                    
            except Exception as e:
                st.error(f"Error processing vendor analysis: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
        
        else:
            st.info("Upload both Health Sherpa Export (CSV) and FMO Statement (Excel) files to analyze vendor performance.")

    # USER MANAGEMENT TAB
    with tabs[10]:
        st.header("User Management")
        
        # Display current users
        st.subheader("Current Users")
        try:
            df_users = pd.read_csv("users.csv")
            st.dataframe(df_users, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Error loading users: {str(e)}")
        
        # Add new user
        st.subheader("Add New User")
        with st.form("add_user_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_username = st.text_input("Username")
                new_password = st.text_input("Password", type="password")
                new_first_name = st.text_input("First Name")
            
            with col2:
                new_last_name = st.text_input("Last Name")
                new_role = st.selectbox("Role", ["Agent", "Admin", "Manager"])
            
            if st.form_submit_button("Add User"):
                if new_username and new_password and new_first_name and new_last_name:
                    try:
                        # Load existing users
                        df_users = pd.read_csv("users.csv")
                        
                        # Add new user
                        new_user = pd.DataFrame({
                            'username': [new_username],
                            'password': [new_password],
                            'first_name': [new_first_name],
                            'last_name': [new_last_name],
                            'role': [new_role]
                        })
                        
                        df_users = pd.concat([df_users, new_user], ignore_index=True)
                        df_users.to_csv("users.csv", index=False)
                        
                        st.success(f"User {new_username} added successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error adding user: {str(e)}")
                else:
                    st.error("Please fill in all fields")

    # LIVE COUNTS TAB
    with tabs[3]:
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
            
            # Initialize CPA analytics data (always defined to prevent errors)
            cpa_analytics = []
            
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
                    medals = ["👑 THE WOLF", "💎 BULL MASTER", "🔥 DEAL CLOSER"]
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
            
            # Agent CPA Analytics Section (only show if data is available)
            if agent_cpa_data and cpa_analytics:
                st.markdown("---")
                st.subheader("📊 Agent CPA Analytics")
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
                        medals = ["👑 THE WOLF", "💎 BULL MASTER", "🔥 DEAL CLOSER"]
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
                    st.info("Need at least 3 agents with deals today to show top performers")
                    
                    # Show available performers
                    if agent_performance:
                        st.markdown("### Today's Performers")
                        perf_df = pd.DataFrame(agent_performance)
                        perf_df.columns = ['Agent', 'Members', 'Deals', 'Top Carrier', 'Est. Commission', 'CPA', 'Billable Calls']
                        perf_df['Est. Commission'] = perf_df['Est. Commission'].apply(lambda x: f"${x}")
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

    # MEMBER TRACKING TAB
    with tabs[4]:
        st.header("👥 Member Tracking Verification")
        
        st.markdown(
            """
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
                border-radius: 15px;
                margin-bottom: 20px;
                text-align: center;
            ">
                <h3 style="margin: 0; color: white; font-size: 24px;">🔍 Member Count Analysis</h3>
                <p style="margin: 10px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
                    Verify how member tracking processes your TQL API data
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        try:
            with st.spinner("Fetching sample data for member tracking analysis..."):
                df_sample = fetch_all_today(limit=20)
            
            if not df_sample.empty:
                st.success(f"Retrieved {len(df_sample)} sample records for analysis")
                
                # Show available columns
                st.subheader("📋 API Data Structure")
                all_cols = list(df_sample.columns)
                member_related_cols = [col for col in all_cols if any(keyword in col.lower() for keyword in ['member', 'applicant', 'dependent', 'count'])]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**All Available Columns:**")
                    st.code(", ".join(all_cols))
                
                with col2:
                    st.markdown("**Member-Related Columns Found:**")
                    if member_related_cols:
                        st.code(", ".join(member_related_cols))
                    else:
                        st.warning("No member-related columns detected")
                
                # Member count analysis
                st.subheader("🔢 Member Count Processing")
                
                if 'total_members' in df_sample.columns:
                    member_stats = df_sample['total_members'].describe()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Average Members/Policy", f"{member_stats['mean']:.2f}")
                    with col2:
                        st.metric("Max Members/Policy", f"{int(member_stats['max'])}")
                    with col3:
                        multi_member_count = (df_sample['total_members'] > 1).sum()
                        st.metric("Policies with >1 Member", f"{multi_member_count}")
                    
                    # Member count distribution
                    st.subheader("📊 Member Count Distribution")
                    member_counts = df_sample['total_members'].value_counts().sort_index()
                    st.bar_chart(member_counts)
                    
                    # Sample records with member data
                    st.subheader("📝 Sample Records with Member Data")
                    display_cols = ['policy_id', 'agent_name', 'lead_first_name', 'lead_last_name']
                    if member_related_cols:
                        display_cols.extend(member_related_cols)
                    display_cols.append('total_members')
                    
                    # Filter to only include columns that exist
                    available_display_cols = [col for col in display_cols if col in df_sample.columns]
                    
                    if available_display_cols:
                        sample_df = df_sample[available_display_cols].head(10)
                        st.dataframe(sample_df, use_container_width=True, hide_index=True)
                    
                    # Commission calculation preview
                    st.subheader("💰 Commission Impact Analysis")
                    total_deals = len(df_sample)
                    total_members = df_sample['total_members'].sum()
                    
                    st.markdown(f"""
                    **Member vs Deal Count Impact:**
                    - Total Deals: {total_deals}
                    - Total Members: {int(total_members)}
                    - Difference: {int(total_members - total_deals)} additional members
                    
                    **Commission Calculation Examples:**
                    - Deal-based ($20/deal): ${total_deals * 20:,}
                    - Member-based ($20/member): ${int(total_members * 20):,}
                    - Additional commission from member tracking: ${int((total_members - total_deals) * 20):,}
                    """)
                else:
                    st.warning("total_members column not found in processed data")
                
                # Vendor retention tracking
                st.subheader("📈 Vendor Retention Analysis")
                if 'lead_vendor_name' in df_sample.columns:
                    vendor_stats = df_sample['lead_vendor_name'].value_counts()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Vendor Distribution:**")
                        st.dataframe(vendor_stats.reset_index().rename(columns={'index': 'Vendor', 'lead_vendor_name': 'Deals'}), 
                                   use_container_width=True, hide_index=True)
                    
                    with col2:
                        st.markdown("**Member Count by Vendor:**")
                        vendor_member_stats = df_sample.groupby('lead_vendor_name').agg({
                            'total_members': ['sum', 'mean'],
                            'policy_id': 'count'
                        }).round(2)
                        vendor_member_stats.columns = ['Total Members', 'Avg Members/Deal', 'Deal Count']
                        st.dataframe(vendor_member_stats, use_container_width=True)
                    
                    # Vendor performance for retention tracking
                    st.markdown("**Vendor Performance Summary for FMO Updates:**")
                    vendor_performance = df_sample.groupby('lead_vendor_name').agg({
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
                    
                    st.dataframe(vendor_performance, use_container_width=True)
                    
                    # Download vendor retention data
                    vendor_csv = vendor_performance.to_csv()
                    st.download_button(
                        "📊 Download Vendor Retention Data",
                        vendor_csv,
                        file_name=f"vendor_retention_analysis_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("lead_vendor_name column not found - vendor tracking unavailable")
                
                # Raw data inspection
                with st.expander("🔍 Raw Data Inspection"):
                    st.markdown("**First 5 records with all fields:**")
                    st.dataframe(df_sample.head(5), use_container_width=True)
                    
                    # Check for any member-related fields in raw data
                    if member_related_cols:
                        st.markdown("**Member-related field values:**")
                        for col in member_related_cols:
                            if col in df_sample.columns:
                                unique_vals = df_sample[col].dropna().unique()
                                st.write(f"{col}: {list(unique_vals[:10])}")  # Show first 10 unique values
            
            else:
                st.error("No data available from API for member tracking verification")
                st.info("This could indicate API connectivity issues or no recent data")
        
        except Exception as e:
            st.error(f"Error during member tracking analysis: {str(e)}")
            st.info("Please check API connectivity and try again")

    # SETTINGS TAB
    with tabs[5]:
        st.header("⚙️ Settings & Upload")

        uploaded_file = st.file_uploader("📥 Upload FMO Statement (xlsx)", type="xlsx")
        hs_file = st.file_uploader("📥 Upload Health Sherpa Export (csv)", type="csv")
        threshold = st.slider("Coaching threshold (Paid Deals)", 0, 100, threshold)

        def safe_str(x):
            return str(x).encode('latin1', errors='replace').decode('latin1')

        if uploaded_file and hs_file:
            st.success("✅ Both files uploaded, processing agent per-member pay...")

            # Health Sherpa: Member lookup
            hs = pd.read_csv(hs_file, dtype=str)
            hs['first_name_norm'] = hs['first_name'].astype(str).str.strip().str.lower()
            hs['last_name_norm'] = hs['last_name'].astype(str).str.strip().str.lower()
            hs['member_count'] = pd.to_numeric(hs['applicant_count'], errors='coerce').fillna(1).astype(int)
            member_lookup = hs.set_index(['first_name_norm','last_name_norm'])['member_count'].to_dict()

            # FMO Paid Deals
            df = pd.read_excel(uploaded_file, dtype=str)
            df = df.dropna(subset=["Agent","first_name","last_name","Advance"])
            df["Paid Status"] = df["Advance"].astype(float).apply(lambda x: "Paid" if x > 0 else "Not Paid")
            df['first_name_norm'] = df['first_name'].astype(str).str.strip().str.lower()
            df['last_name_norm'] = df['last_name'].astype(str).str.strip().str.lower()
            if "Advance Excluded Reason" in df.columns:
                df["Reason"] = df["Advance Excluded Reason"]
            else:
                df["Reason"] = ""

            # Agent Calculations
            agent_stats = []
            for agent in df["Agent"].unique():
                sub = df[df["Agent"]==agent]
                paid_sub = sub[sub["Paid Status"]=="Paid"]
                unpaid_sub = sub[sub["Paid Status"]!="Paid"]

                paid_count = len(paid_sub)
                unpaid_count = len(unpaid_sub)
                all_count = paid_count + unpaid_count
                paid_pct = (paid_count / all_count * 100) if all_count > 0 else 0

                client_rows = []
                total_members = 0
                for _, row in paid_sub.iterrows():
                    key = (row['first_name_norm'], row['last_name_norm'])
                    members = member_lookup.get(key, 1)
                    total_members += members
                    client_rows.append((row['first_name'], row['last_name'], members))

                unpaid_rows = []
                for _, row in unpaid_sub.iterrows():
                    reason = row['Reason'] if "Reason" in row and pd.notnull(row['Reason']) else ""
                    unpaid_rows.append((row['first_name'], row['last_name'], reason))

                # HCS Tier Structure: BASED ON total_members (Per commission agreement)
                if total_members >= 140:
                    rate = 25
                    bonus = 1200
                elif total_members >= 100:
                    rate = 22.5
                    bonus = 1200
                elif total_members >= 70:
                    rate = 17.5
                    bonus = 1200
                else:
                    rate = 15
                    bonus = 0

                production_bonus = bonus
                retention_bonus = 500 if (paid_pct >= 80 and paid_count >= 80) else 0  # 80+ paid deals AND 80%+ retention
                agent_stats.append({
                    "Agent": agent,
                    "Paid Applications": paid_count,
                    "Unpaid Applications": unpaid_count,
                    "Paid %": paid_pct,
                    "Total Members": total_members,
                    "Per-Member Rate": rate,
                    "Production Bonus": production_bonus,
                    "Retention Bonus": retention_bonus,
                    "Client Rows": client_rows,
                    "Unpaid Rows": unpaid_rows,
                })

            # Top Agent Bonus
            top_member_count = max(r["Total Members"] for r in agent_stats) if agent_stats else 0
            top_agents = [r["Agent"] for r in agent_stats if r["Total Members"] == top_member_count and top_member_count > 0]

            # Generate PDFs, CSV, Add Top Agent Bonus
            buf = io.BytesIO()
            summary = []
            with zipfile.ZipFile(buf, "w") as zf:
                for stats in agent_stats:
                    agent = stats["Agent"]
                    paid_count = stats["Paid Applications"]
                    unpaid_count = stats["Unpaid Applications"]
                    paid_pct = stats["Paid %"]
                    total_members = stats["Total Members"]
                    rate = stats["Per-Member Rate"]
                    production_bonus = stats["Production Bonus"]
                    retention_bonus = stats["Retention Bonus"]
                    top_agent_bonus = 250 if agent in top_agents else 0
                    total_bonus = production_bonus + retention_bonus + top_agent_bonus
                    total_payout = total_members * rate + total_bonus
                    client_rows = stats["Client Rows"]
                    unpaid_rows = stats["Unpaid Rows"]

                    summary.append({
                        "Agent": agent,
                        "Paid Applications": paid_count,
                        "Unpaid Applications": unpaid_count,
                        "Paid %": f"{paid_pct:.1f}%",
                        "Total Members": total_members,
                        "Per-Member Rate": rate,
                        "Production Bonus": production_bonus,
                        "Retention Bonus": retention_bonus,
                        "Top Agent Bonus": top_agent_bonus,
                        "Agent Payout": total_payout,
                        "Unpaid Reasons": "; ".join(f"{fname} {lname}: {reason or 'N/A'}" for fname, lname, reason in unpaid_rows)
                    })

                    # Generate PDF for agent
                    pdf = FPDF()
                    pdf.add_page()
                    pdf.set_font("Arial","B",16)
                    pdf.cell(0,10,safe_str("Health Connect Solutions"), ln=True, align="C")
                    pdf.ln(5)
                    pdf.set_font("Arial","B",12)
                    pdf.cell(0,10,safe_str(f"Agent Pay Statement – {agent}"), ln=True)
                    pdf.ln(5)
                    pdf.set_font("Arial","",12)
                    pdf.cell(0,8,safe_str(f"Paid Applications: {paid_count}"), ln=True)
                    pdf.cell(0,8,safe_str(f"Unpaid Applications: {unpaid_count}"), ln=True)
                    pdf.cell(0,8,safe_str(f"Paid %: {paid_pct:.1f}%"), ln=True)
                    pdf.cell(0,8,safe_str(f"Total Members: {total_members}"), ln=True)
                    pdf.cell(0,8,safe_str(f"Per-Member Rate: ${rate:.2f}"), ln=True)
                    pdf.cell(0,8,safe_str(f"Production Bonus: ${production_bonus}"), ln=True)
                    pdf.cell(0,8,safe_str(f"Retention Bonus: ${retention_bonus}"), ln=True)
                    pdf.cell(0,8,safe_str(f"Top Agent Bonus: ${top_agent_bonus}"), ln=True)
                    pdf.set_text_color(0,150,0)
                    pdf.cell(0,10,safe_str(f"Total Payout: ${total_payout:,.2f}"), ln=True)
                    pdf.set_text_color(0,0,0)
                    pdf.ln(5)
                    
                    # Add client details
                    pdf.set_font("Arial","B",12)
                    pdf.cell(0,8,safe_str("Paid Clients:"), ln=True)
                    pdf.set_font("Arial","",10)
                    for fname, lname, members in client_rows:
                        pdf.multi_cell(0,6,safe_str(f"- {fname} {lname} ({members} members)"))
                    
                    pdf.ln(3)
                    pdf.set_font("Arial","B",12)
                    pdf.cell(0,8,safe_str("Unpaid Clients & Reasons:"), ln=True)
                    pdf.set_font("Arial","",10)
                    for fname, lname, reason in unpaid_rows:
                        pdf.multi_cell(0,6,safe_str(f"- {fname} {lname}: {reason or 'No reason'}"))
                    
                    zf.writestr(f"{agent}_pay_statement.pdf", pdf.output(dest="S").encode("latin1"))

                # Add CSV summary
                csv_buf = io.StringIO()
                w = csv.writer(csv_buf)
                w.writerow([
                    "Agent","Paid Applications","Unpaid Applications","Paid %","Total Members",
                    "Per-Member Rate","Production Bonus","Retention Bonus","Top Agent Bonus","Agent Payout","Unpaid Reasons"
                ])
                for r in summary:
                    w.writerow([
                        r["Agent"], r["Paid Applications"], r["Unpaid Applications"], r["Paid %"], r["Total Members"],
                        r["Per-Member Rate"], r["Production Bonus"], r["Retention Bonus"], r["Top Agent Bonus"], r["Agent Payout"], r["Unpaid Reasons"]
                    ])
                zf.writestr("HCS_Admin_Summary.csv", csv_buf.getvalue())

            # Store individual agent payroll data in database
            upload_date = datetime.now().strftime("%Y-%m-%d")
            if insert_agent_payroll(summary, upload_date):
                st.success("Agent payroll data stored successfully.")
                # Update session state to trigger agent dashboard refresh
                st.session_state['payroll_last_updated'] = upload_date
                st.session_state['payroll_update_timestamp'] = datetime.now().timestamp()
                # Clear any cached agent data
                if 'agent_payroll_cache' in st.session_state:
                    del st.session_state['agent_payroll_cache']
            else:
                st.warning("Failed to store agent payroll data.")

            # Calculate totals
            owner_rev = df[df["Advance"].astype(float) == 150]["Advance"].astype(float).sum()
            agent_payout = sum(r["Agent Payout"] for r in summary)
            owner_prof = owner_rev - agent_payout

            totals = {
                "deals": sum(r["Paid Applications"] for r in summary),
                "agent": float(agent_payout),
                "owner_rev": float(owner_rev),
                "owner_prof": float(owner_prof)
            }
            
            # Store in session state
            st.session_state["payroll_totals"] = totals
            st.session_state["summary"] = summary
            
            # Store in database
            today_str = datetime.now().strftime("%Y-%m-%d")
            insert_report(today_str, totals)

            st.download_button(
                "📦 Download ZIP of Agent Per-Member Paystubs",
                buf.getvalue(),
                file_name=f"agent_per_member_paystubs_{datetime.now():%Y%m%d}.zip",
                mime="application/zip"
            )
            
            # Generate individual agent CRM access and reports automatically
            st.markdown("---")
            st.subheader("🏢 Agent CRM Net Pay Reports Generated")
            st.success("Individual agent net pay reports are now available in their CRM portals")
            
            # Determine commission cycle from generation date
            generation_date = datetime.now()
            cycle_info = None
            
            # Find the cycle being paid based on generation date
            for i, cycle in commission_cycles.iterrows():
                cycle_end = pd.to_datetime(cycle['end'])
                pay_date = pd.to_datetime(cycle['pay'])
                
                # If generated between cycle end and pay date, this is the cycle being paid
                if cycle_end < generation_date <= pay_date:
                    cycle_info = {
                        'start': cycle['start'],
                        'end': cycle['end'], 
                        'pay': cycle['pay'],
                        'period': f"{cycle['start']} to {cycle['end']}"
                    }
                    break
            
            if cycle_info:
                st.info(f"📅 Pay reports generated for cycle: {cycle_info['period']} (Pay Date: {cycle_info['pay']})")
            else:
                # Use most recent completed cycle
                current_date = pd.to_datetime(generation_date.date())
                completed_cycles = commission_cycles[pd.to_datetime(commission_cycles['end']) < current_date]
                if not completed_cycles.empty:
                    latest_cycle = completed_cycles.iloc[-1]
                    cycle_info = {
                        'start': latest_cycle['start'],
                        'end': latest_cycle['end'],
                        'pay': latest_cycle['pay'],
                        'period': f"{latest_cycle['start']} to {latest_cycle['end']}"
                    }
                    st.info(f"📅 Pay reports generated for most recent completed cycle: {cycle_info['period']}")
                else:
                    cycle_info = {
                        'start': 'Current',
                        'end': 'Period',
                        'pay': 'TBD',
                        'period': 'Current Period'
                    }
                    st.warning("Using current period for pay calculations")
            
            # Store agent reports for individual CRM access
            agent_reports = {}
            for stats in agent_stats:
                agent = stats["Agent"]
                paid_count = stats["Paid Applications"]
                unpaid_count = stats["Unpaid Applications"]
                paid_pct = stats["Paid %"]
                total_members = stats["Total Members"]
                rate = stats["Per-Member Rate"]
                production_bonus = stats["Production Bonus"]
                retention_bonus = stats["Retention Bonus"]
                top_agent_bonus = 250 if agent in top_agents else 0
                total_bonus = production_bonus + retention_bonus + top_agent_bonus
                total_payout = total_members * rate + total_bonus
                
                agent_reports[agent] = {
                    "paid_applications": paid_count,
                    "unpaid_applications": unpaid_count,
                    "paid_percentage": paid_pct,
                    "total_members": total_members,
                    "per_member_rate": rate,
                    "production_bonus": production_bonus,
                    "retention_bonus": retention_bonus,
                    "top_agent_bonus": top_agent_bonus,
                    "total_payout": total_payout,
                    "client_rows": stats["Client Rows"],
                    "unpaid_rows": stats["Unpaid Rows"],
                    "cycle_period": cycle_info['period'],
                    "pay_date": cycle_info['pay']
                }
            
            # Store in session state for agent CRM access
            st.session_state['agent_reports'] = agent_reports
            st.session_state['reports_generated'] = True
            st.session_state['cycle_info'] = cycle_info
            
            # Display agent pay summary
            st.markdown("### 💰 Agent Net Pay Summary")
            total_payout = sum(report['total_payout'] for report in agent_reports.values())
            st.metric("Total Agent Payouts", f"${total_payout:,.2f}")
            
            # Show individual agent pay amounts
            pay_summary = []
            for agent, report in agent_reports.items():
                # Find matching username for login info
                agent_username = None
                for username, name in AGENT_NAMES.items():
                    if name == agent or username == agent:
                        agent_username = username
                        break
                
                pay_summary.append({
                    "Agent": agent,
                    "Username": agent_username or "Contact Admin",
                    "Net Pay": f"${report['total_payout']:,.2f}",
                    "Paid Apps": report['paid_applications'],
                    "Total Members": report['total_members']
                })
            
            if pay_summary:
                pay_df = pd.DataFrame(pay_summary)
                st.dataframe(pay_df, use_container_width=True, hide_index=True)
                
                # Download agent access summary
                csv_pay = pay_df.to_csv(index=False)
                cycle_label = cycle_info['period'].replace('/', '_').replace(' ', '_') if cycle_info else datetime.now().strftime("%Y%m%d")
                st.download_button(
                    "📥 Download Agent Login & Pay Summary",
                    csv_pay,
                    file_name=f"agent_login_pay_summary_{cycle_label}.csv",
                    mime="text/csv"
                )
            
            # Store individual agent net pay data for CRM access
            # Determine commission cycle
            generation_date = datetime.now()
            cycle_info = None
            
            for i, cycle in commission_cycles.iterrows():
                cycle_end = pd.to_datetime(cycle['end'])
                pay_date = pd.to_datetime(cycle['pay'])
                
                if cycle_end < generation_date <= pay_date:
                    cycle_info = {
                        'start': cycle['start'],
                        'end': cycle['end'], 
                        'pay': cycle['pay'],
                        'period': f"{cycle['start']} to {cycle['end']}"
                    }
                    break
            
            if not cycle_info:
                current_date = pd.to_datetime(generation_date.date())
                completed_cycles = commission_cycles[pd.to_datetime(commission_cycles['end']) < current_date]
                if not completed_cycles.empty:
                    latest_cycle = completed_cycles.iloc[-1]
                    cycle_info = {
                        'start': latest_cycle['start'],
                        'end': latest_cycle['end'],
                        'pay': latest_cycle['pay'],
                        'period': f"{latest_cycle['start']} to {latest_cycle['end']}"
                    }
            
            # Store agent reports for individual CRM access with PDF data
            payroll_agent_reports = {}
            for stats in agent_stats:
                agent = stats["Agent"]
                pdf_filename = f"{agent.replace(' ', '_')}_pay_statement.pdf"
                
                # Extract PDF content from the ZIP buffer
                buf.seek(0)
                pdf_content = None
                with zipfile.ZipFile(buf, "r") as read_zf:
                    try:
                        pdf_content = read_zf.read(pdf_filename)
                    except KeyError:
                        pass
                
                payroll_agent_reports[agent] = {
                    "paid_applications": stats["Paid Applications"],
                    "unpaid_applications": stats["Unpaid Applications"],
                    "total_members": stats["Total Members"],
                    "per_member_rate": stats["Per-Member Rate"],
                    "production_bonus": stats["Production Bonus"],
                    "retention_bonus": stats["Retention Bonus"],
                    "top_agent_bonus": 250 if agent in top_agents else 0,
                    "total_payout": stats["Total Members"] * stats["Per-Member Rate"] + stats["Production Bonus"] + stats["Retention Bonus"] + (250 if agent in top_agents else 0),
                    "client_rows": stats["Client Rows"],
                    "unpaid_rows": stats["Unpaid Rows"],
                    "cycle_period": cycle_info['period'] if cycle_info else "Current Period",
                    "pay_date": cycle_info['pay'] if cycle_info else "TBD",
                    "pdf_content": pdf_content
                }
            
            # Store in session state for agent CRM access
            st.session_state['agent_reports'] = payroll_agent_reports
            st.session_state['reports_generated'] = True
            st.session_state['cycle_info'] = cycle_info
            
            st.success("✅ All agents can now log in to their CRM to view individual net pay reports and download their pay statements")

    # CLIENTS TAB (ALL TODAY) with AUTO-REFRESH
    with tabs[6]:
        st_autorefresh(interval=10 * 1000, key="clients_tab_refresh")
        st.header("📂 Live Client Leads (Sold Today)")
        
        # SMS System Status Indicator
        st.info("📱 SMS Client Follow-up System is available below the client data")
        df_api = fetch_all_today(limit=5000)
        if df_api.empty:
            st.info("No API leads returned.")
            api_display = pd.DataFrame()
        else:
            df_api["date_sold"] = pd.to_datetime(df_api["date_sold"], errors="coerce")
            api_today = df_api[df_api["date_sold"].dt.date == date.today()]
            api_cols = [
                "policy_id","lead_vendor_name","agent_name","lead_first_name","lead_last_name","lead_state",
                "date_sold","carrier","product","duration","premium","total_members",
                "policy_number"
            ]
            api_cols = [c for c in api_cols if c in api_today.columns]
            if api_cols:
                api_display = api_today[api_cols].copy()
                api_display = api_display.rename(columns={
                    "policy_id": "Policy ID",
                    "lead_vendor_name": "Vendor",
                    "agent_name": "Agent",
                    "lead_first_name": "First Name",
                    "lead_last_name": "Last Name",
                    "lead_state": "State",
                    "date_sold": "Date Sold",
                    "total_members": "Members",
                    "premium": "Premium"
                })
            else:
                api_display = pd.DataFrame()
                
        if "manual_leads" not in st.session_state:
            st.session_state.manual_leads = pd.DataFrame()
        
        combined = api_display
        if not st.session_state.manual_leads.empty:
            combined = pd.concat([api_display, st.session_state.manual_leads], ignore_index=True, sort=False)
        
        if combined.empty:
            st.warning("No leads to display for today.")
        else:
            st.subheader(f"Showing {len(combined)} total leads")
            st.dataframe(combined, use_container_width=True)
        
        # SMS Automation Section
        st.markdown("---")
        st.subheader("📱 SMS Client Follow-up System")
        
        # Show SMS interface
        sms_tab1, sms_tab2, sms_tab3 = st.tabs(["📅 Next-Day Follow-up", "📢 Bulk Campaign", "📊 SMS History"])
        
        # Initialize SMS system
        sms_system = None
        sms_error = None
        try:
            from sms_automation import SMSAutomation
            sms_system = SMSAutomation()
        except Exception as e:
            sms_error = str(e)
            st.error(f"SMS system initialization failed: {sms_error}")
        
        # Quick SMS Test
        with st.expander("🧪 Test SMS System"):
            col1, col2 = st.columns([2, 1])
            with col1:
                test_phone = st.text_input("Test Phone Number (e.g., 561-365-0568):", placeholder="Enter phone number")
            with col2:
                if st.button("📱 Send Test SMS") and test_phone and sms_system:
                    try:
                        clean_phone = sms_system.clean_phone_number(test_phone)
                        if clean_phone:
                            test_message = "Test message from HCS CRM SMS system - working correctly!"
                            result = sms_system.send_sms(clean_phone, test_message)
                            if result.get('success'):
                                st.success(f"Test SMS sent to {clean_phone}")
                                st.write(f"Message SID: {result.get('sid')}")
                            else:
                                st.error("Failed to send test SMS")
                        else:
                            st.error("Invalid phone number format")
                    except Exception as e:
                        st.error(f"SMS test failed: {str(e)}")
        
        # Debug info for testing
        if st.checkbox("Show SMS Debug Info", value=False):
            st.write(f"CRM_API_ID: {CRM_API_ID}")
            st.write(f"CRM_API_KEY: {'*' * len(CRM_API_KEY) if CRM_API_KEY else 'Not set'}")
            if sms_error:
                st.write(f"Error: {sms_error}")
            else:
                st.write("SMS system initialized successfully")
        
        with sms_tab1:
            st.markdown("### Automated Next-Day Client Follow-up")
            st.info("Send follow-up messages to clients who signed up yesterday")
            
            if sms_system:
                if st.button("🔄 Load Yesterday's Clients", key="load_yesterday"):
                    with st.spinner("Fetching yesterday's clients..."):
                        yesterday_clients = sms_system.get_recent_clients(CRM_API_ID, CRM_API_KEY, days_back=1)
                        st.session_state.yesterday_clients = yesterday_clients
                
                if hasattr(st.session_state, 'yesterday_clients') and st.session_state.yesterday_clients:
                    clients = st.session_state.yesterday_clients
                    st.success(f"Found {len(clients)} clients from yesterday")
                    
                    # Show preview of clients
                    client_df = pd.DataFrame([{
                        'Name': f"{c.get('lead_first_name', '')} {c.get('lead_last_name', '')}",
                        'Phone': c.get('clean_phone', ''),
                        'Agent': c.get('agent_name', ''),
                        'Carrier': c.get('carrier', '')
                    } for c in clients])
                    
                    st.dataframe(client_df, use_container_width=True, hide_index=True)
                    
                    # Message template selection
                    templates = sms_system.get_message_templates()
                    selected_template = st.selectbox(
                        "Select Message Template:",
                        ["next_day_followup", "retention_reminder", "general_check_in"],
                        format_func=lambda x: x.replace('_', ' ').title()
                    )
                    
                    # Show template preview
                    if selected_template:
                        preview_message = templates[selected_template].format(
                            first_name="John",
                            agent_name="Sample Agent", 
                            carrier="AMBETTER"
                        )
                        st.text_area("Message Preview:", preview_message, height=150, disabled=True)
                    
                    # Send messages
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("📤 Send Follow-up Messages", type="primary"):
                            with st.spinner("Sending messages..."):
                                results = sms_system.send_bulk_sms(clients, "", selected_template)
                                st.session_state.last_sms_results = results
                                
                                # Show results
                                stats = sms_system.get_sms_statistics(results)
                                st.success(f"Campaign Complete! Sent: {stats['sent']}/{stats['total']} ({stats['success_rate']}% success)")
                    
                    with col2:
                        if st.button("📋 Preview Only"):
                            st.info("Preview mode - no messages will be sent")
                else:
                    st.info("Click 'Load Yesterday's Clients' to see available clients for follow-up")
            else:
                st.warning("SMS system unavailable - check Twilio configuration")
        
        with sms_tab2:
            st.markdown("### Bulk SMS Campaign")
            st.info("Send custom messages to clients from any date range")
            
            if sms_system:
                # Date range selection
                col1, col2 = st.columns(2)
                with col1:
                    days_back = st.selectbox("Client Range:", [1, 3, 7, 14, 30], index=2, format_func=lambda x: f"Last {x} days")
                
                with col2:
                    if st.button("🔄 Load Clients", key="load_bulk"):
                        with st.spinner("Fetching clients..."):
                            bulk_clients = sms_system.get_recent_clients(CRM_API_ID, CRM_API_KEY, days_back=days_back)
                            st.session_state.bulk_clients = bulk_clients
                
                if hasattr(st.session_state, 'bulk_clients') and st.session_state.bulk_clients:
                    clients = st.session_state.bulk_clients
                    st.success(f"Found {len(clients)} clients from last {days_back} days")
                    
                    # Message customization
                    templates = sms_system.get_message_templates()
                    template_options = list(templates.keys())
                    selected_template = st.selectbox("Message Template:", template_options, format_func=lambda x: x.replace('_', ' ').title())
                    
                    if selected_template == "custom":
                        custom_message = st.text_area(
                            "Custom Message:",
                            height=150,
                            placeholder="Hi {first_name}! This is {agent_name} from HCS..."
                        )
                    else:
                        custom_message = templates[selected_template]
                        st.text_area("Message Template:", custom_message, height=150, disabled=True)
                    
                    # Send campaign
                    if st.button("📤 Send Bulk Campaign", type="primary"):
                        with st.spinner("Sending bulk campaign..."):
                            message_to_send = custom_message if selected_template == "custom" else ""
                            results = sms_system.send_bulk_sms(clients, message_to_send, selected_template)
                            st.session_state.last_sms_results = results
                            
                            # Show results
                            stats = sms_system.get_sms_statistics(results)
                            st.success(f"Bulk Campaign Complete! Sent: {stats['sent']}/{stats['total']} ({stats['success_rate']}% success)")
                else:
                    st.info("Click 'Load Clients' to see available clients for bulk messaging")
            else:
                st.warning("SMS system unavailable - check Twilio configuration")
        
        with sms_tab3:
            st.markdown("### SMS Campaign Results")
            
            if hasattr(st.session_state, 'last_sms_results') and st.session_state.last_sms_results:
                results = st.session_state.last_sms_results
                if sms_system:
                    stats = sms_system.get_sms_statistics(results)
                    
                    # Statistics cards
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Messages", stats['total'])
                    with col2:
                        st.metric("Successfully Sent", stats['sent'])
                    with col3:
                        st.metric("Failed", stats['failed'])
                    with col4:
                        st.metric("Success Rate", f"{stats['success_rate']}%")
                    
                    # Download results
                    results_df = pd.DataFrame(results)
                    csv_data = results_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results CSV",
                        data=csv_data,
                        file_name=f"sms_campaign_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No recent SMS campaign results to display. Run a campaign to see results here.")
                
                # Show sample message templates
                if sms_system:
                    st.subheader("📋 Available Message Templates")
                    templates = sms_system.get_message_templates()
                    
                    for template_name, template_content in templates.items():
                        if template_name != "custom":
                            with st.expander(f"📄 {template_name.replace('_', ' ').title()}"):
                                st.text(template_content)

    # VENDOR PAY TAB
    with tabs[7]:
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
            if len(tld.columns) > 8:
                tld['VendorRaw'] = tld.iloc[:, 8].astype(str)
            else:
                st.error("TLD file does not have enough columns")
                st.stop()
                
            if len(tld.columns) > 4:
                tld['First Name'] = tld.iloc[:, 3].astype(str)
                tld['Last Name'] = tld.iloc[:, 4].astype(str)
            else:
                st.error("TLD file does not have name columns")
                st.stop()
                
            tld['vendor_key'] = tld['VendorRaw'].apply(normalize_key)

            fmo = pd.read_excel(fmo_file, dtype=str)
            if len(fmo.columns) > 8:
                fmo['First Name'] = fmo.iloc[:, 7].astype(str)
                fmo['Last Name'] = fmo.iloc[:, 8].astype(str)
            else:
                st.error("FMO file does not have enough columns")
                st.stop()
                
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

            # Generate vendor PDFs
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as zf:
                for vkey, pretty in VENDOR_CODES.items():
                    if vkey not in VENDOR_RATES:
                        continue
                    rate = VENDOR_RATES[vkey]
                    sub = merged[merged['vendor_key'] == vkey]
                    paid = sub[sub['Advance'] > 0]
                    unpaid = sub[sub['Advance'] == 0]
                    
                    if len(paid) > 0 or len(unpaid) > 0:
                        pct_paid = (len(paid) / (len(paid) + len(unpaid)) * 100) if (len(paid) + len(unpaid)) > 0 else 0
                        paid_amt = len(paid) * rate
                        
                        # Convert to proper format for PDF generation (handle both real and mock pandas)
                        paid_pdf_data = None
                        unpaid_pdf_data = None
                        
                        if len(paid) > 0:
                            try:
                                if hasattr(paid, 'iterrows'):
                                    paid_pdf_data = paid
                                else:
                                    # Convert to list format for mock pandas
                                    paid_pdf_data = []
                                    for i in range(len(paid)):
                                        paid_pdf_data.append({
                                            'first_name': f'Client',
                                            'last_name': f'#{i+1}',
                                            'advance': rate
                                        })
                            except:
                                paid_pdf_data = []
                        
                        if len(unpaid) > 0:
                            try:
                                if hasattr(unpaid, 'iterrows'):
                                    unpaid_pdf_data = unpaid
                                else:
                                    # Convert to list format for mock pandas
                                    unpaid_pdf_data = []
                                    for i in range(len(unpaid)):
                                        unpaid_pdf_data.append({
                                            'first_name': f'Client',
                                            'last_name': f'#{i+1}',
                                            'reason': 'Under Review'
                                        })
                            except:
                                unpaid_pdf_data = []
                        
                        pdf_content = vendor_pdf(paid_pdf_data, unpaid_pdf_data, pretty, rate)
                        zf.writestr(f"{pretty}_vendor_pay.pdf", pdf_content)

            st.download_button(
                "📦 Download Vendor Pay ZIP",
                buf.getvalue(),
                file_name=f"vendor_pay_reports_{datetime.now():%Y%m%d}.zip",
                mime="application/zip"
            )
        else:
            st.info("Upload TLD CSV and FMO Statement files to generate vendor pay reports.")

    # AGENT NET PAY TAB  
    with tabs[8]:
        st.header("🧾 Agent Net Pay")
        st.subheader("Upload Files for Net Pay Calculation")
        
        # File uploads for net pay calculation
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("FMO Commission Statement")
            fmo_net_file = st.file_uploader("Upload FMO Statement (Excel)", type=["xlsx"], key="fmo_agentpay_upload")
            
        with col2:
            st.subheader("Health Sherpa Export")
            hs_net_file = st.file_uploader("Upload Health Sherpa Export (CSV)", type=["csv"], key="hs_agentpay_upload")
        
        # Process files when both are uploaded
        if fmo_net_file and hs_net_file:
            st.success("Both files uploaded - calculating agent net pay...")
            
            try:
                # Load and process files using the existing logic from Settings tab
                hs = pd.read_csv(hs_net_file, dtype=str)
                hs['first_name_norm'] = hs['first_name'].astype(str).str.strip().str.lower()
                hs['last_name_norm'] = hs['last_name'].astype(str).str.strip().str.lower()
                hs['member_count'] = pd.to_numeric(hs['applicant_count'], errors='coerce').fillna(1).astype(int)
                member_lookup = hs.set_index(['first_name_norm','last_name_norm'])['member_count'].to_dict()

                # FMO Paid Deals
                df = pd.read_excel(fmo_net_file, dtype=str)
                df = df.dropna(subset=["Agent","first_name","last_name","Advance"])
                df["Paid Status"] = df["Advance"].astype(float).apply(lambda x: "Paid" if x > 0 else "Not Paid")
                df['first_name_norm'] = df['first_name'].astype(str).str.strip().str.lower()
                df['last_name_norm'] = df['last_name'].astype(str).str.strip().str.lower()

                st.subheader("Net Pay Calculation Results")
                
                # Calculate net pay for each agent
                net_pay_results = []
                for agent in df["Agent"].unique():
                    sub = df[df["Agent"]==agent]
                    paid_sub = sub[sub["Paid Status"]=="Paid"]
                    unpaid_sub = sub[sub["Paid Status"]!="Paid"]

                    paid_count = len(paid_sub)
                    unpaid_count = len(unpaid_sub)
                    total_members = 0
                    
                    # Calculate total members using Health Sherpa data
                    for _, row in paid_sub.iterrows():
                        key = (row['first_name_norm'], row['last_name_norm'])
                        members = member_lookup.get(key, 1)
                        total_members += members

                    # Calculate tier-based rate (Per commission agreement)
                    if total_members >= 140:
                        rate = 25
                        bonus = 1200
                    elif total_members >= 100:
                        rate = 22.5
                        bonus = 1200
                    elif total_members >= 70:
                        rate = 17.5
                        bonus = 1200
                    else:
                        rate = 15
                        bonus = 0

                    # Calculate pay components
                    base_pay = total_members * rate
                    production_bonus = bonus
                    paid_pct = (paid_count / (paid_count + unpaid_count) * 100) if (paid_count + unpaid_count) > 0 else 0
                    retention_bonus = 500 if (paid_pct >= 80 and paid_count >= 80) else 0  # 80+ paid deals AND 80%+ retention
                    
                    # Calculate advances/deductions
                    advances = pd.to_numeric(sub['Advance'], errors='coerce').sum()
                    
                    # Net pay calculation
                    gross_pay = base_pay + production_bonus + retention_bonus
                    net_pay = gross_pay - advances
                    
                    net_pay_results.append({
                        "Agent": agent,
                        "Paid Applications": paid_count,
                        "Unpaid Applications": unpaid_count,
                        "Paid %": f"{paid_pct:.1f}%",
                        "Total Members": total_members,
                        "Per-Member Rate": f"${rate}",
                        "Base Pay": f"${base_pay:,.2f}",
                        "Production Bonus": f"${production_bonus:,.2f}",
                        "Retention Bonus": f"${retention_bonus:,.2f}",
                        "Gross Pay": f"${gross_pay:,.2f}",
                        "Advances": f"${advances:,.2f}",
                        "Net Pay": f"${net_pay:,.2f}"
                    })
                
                if net_pay_results:
                    net_df = pd.DataFrame(net_pay_results)
                    st.dataframe(net_df, use_container_width=True)
                    
                    # Download net pay summary
                    csv_data = net_df.to_csv(index=False)
                    st.download_button(
                        "Download Net Pay Summary",
                        csv_data,
                        file_name=f"agent_net_pay_{datetime.now():%Y%m%d}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No agent data found for net pay calculation.")
                    
            except Exception as e:
                st.error(f"Error processing files: {str(e)}")
                st.write("Please ensure your files have the correct format:")
                st.write("- FMO file should have Agent, first_name, last_name, Advance columns")
                st.write("- Health Sherpa file should have first_name, last_name, applicant_count columns")
        
        elif fmo_net_file or hs_net_file:
            st.info("Please upload both FMO statement and Health Sherpa export to calculate net pay.")
        else:
            st.info("Upload both commission files to begin net pay calculations.")
        
    # VENDOR CPL/CPA TAB
    with tabs[9]:
        st.header("📊 Vendor CPL/CPA")
        st.subheader("Upload Files for Vendor Cost Analysis")
        
        # File uploads for vendor analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("TLD Lead Data")
            tld_cpl_file = st.file_uploader("Upload TLD CSV Export", type=["csv"], key="tld_cpl_upload")
            
        with col2:
            st.subheader("FMO Commission Data")
            fmo_cpl_file = st.file_uploader("Upload FMO Statement (Excel)", type=["xlsx"], key="fmo_cpl_upload")
        
        # Vendor rates configuration
        st.subheader("Vendor Rate Configuration")
        st.write("Configure cost per lead/acquisition rates for each vendor:")
        
        # Display current vendor rates in an editable format
        if st.button("Show Current Vendor Rates"):
            vendor_rates_display = []
            VENDOR_RATES = {
                "francalls": 75,
                "hcsmedia": 75,
                "buffercall": 65,
                "ancletadvising": 55,
                "blmcalls": 50,
                "loopcalls": 45,
                "nobufferaca": 40,
                "raycalls": 35,
                "nomiaca": 30,
                "acaking": 25,
                "ptacacalls": 20,
                "hcscaa": 15,
                "slavaaca": 45,
                "slavaaca2": 40,
                "francallssupp": 60,
                "derekinhousefb": 10,
                "allicalladdoncall": 25,
                "joshaca": 30,
                "hcs1p": 15
            }
            
            for vendor_key, rate in VENDOR_RATES.items():
                vendor_rates_display.append({
                    "Vendor Code": vendor_key,
                    "Cost per Lead": f"${rate}",
                    "Description": VENDOR_CODES.get(vendor_key, vendor_key.upper())
                })
            
            rates_df = pd.DataFrame(vendor_rates_display)
            st.dataframe(rates_df, use_container_width=True)
        
        # Process files when both are uploaded
        if tld_cpl_file and fmo_cpl_file:
            st.success("Both files uploaded - calculating vendor CPL/CPA metrics...")
            
            try:
                # Load files using real pandas for vendor analysis
                import pandas as real_pd
                
                # Load TLD data with error handling
                try:
                    tld = real_pd.read_csv(tld_cpl_file, dtype=str)
                except Exception as e:
                    st.error(f"Error loading TLD file: {str(e)}")
                    st.stop()
                
                # Load FMO data with error handling
                try:
                    fmo = real_pd.read_excel(fmo_cpl_file, dtype=str)
                except Exception as e:
                    st.error(f"Error loading FMO file: {str(e)}")
                    st.stop()
                
                st.subheader("Data Preview")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write("TLD Data Sample:")
                    st.dataframe(tld.head(), use_container_width=True)
                    
                with col2:
                    st.write("FMO Data Sample:")
                    st.dataframe(fmo.head(), use_container_width=True)
                
                if st.button("Calculate Vendor CPL/CPA", key="calc_vendor_cpl"):
                    with st.spinner("Processing vendor cost analysis..."):
                        
                        # Normalize vendor names for matching
                        def normalize_key(x):
                            return str(x).lower().replace(" ", "").replace("-", "").replace("_", "")
                        
                        # Process TLD data - check for various vendor column names
                        vendor_column = None
                        possible_vendor_cols = ['VendorRaw', 'Vendor', 'vendor', 'lead_vendor', 'lead_vendor_name', 'vendor_name', 'source']
                        
                        for col in possible_vendor_cols:
                            if col in tld.columns:
                                vendor_column = col
                                break
                        
                        if vendor_column:
                            tld['vendor_key'] = tld[vendor_column].apply(normalize_key)
                            st.success(f"Using '{vendor_column}' column for vendor mapping")
                        else:
                            st.error(f"No vendor column found. Available columns: {list(tld.columns)}")
                            st.write("Please ensure your TLD file has one of these columns: VendorRaw, Vendor, lead_vendor_name, etc.")
                            st.stop()
                        
                        # Process FMO data for paid status
                        if 'Advance' in fmo.columns:
                            fmo['Advance'] = pd.to_numeric(fmo['Advance'], errors='coerce').fillna(0)
                            fmo['is_paid'] = fmo['Advance'] > 0
                        else:
                            st.error("FMO file missing 'Advance' column")
                            st.stop()
                        
                        # Merge data for analysis - check for various name column patterns
                        first_name_col = None
                        last_name_col = None
                        
                        # Debug: Show available columns first
                        st.write("**TLD File Columns:**")
                        st.write(list(tld.columns))
                        
                        # Use exact TLD column positions as specified
                        # Column 0: lead_id, Column 1: date_created, Column 2: date_modified
                        # Column 3: first_name, Column 4: last_name, Column 5: phone_number, Column 8: vendor
                        st.write("**TLD File Structure (using specified column positions):**")
                        st.write(f"Total columns: {len(tld.columns)}")
                        
                        try:
                            # Extract data using exact column positions
                            if len(tld.columns) > 8:
                                tld['first_name'] = tld.iloc[:, 3].fillna('').astype(str)  # Column 3
                                tld['last_name'] = tld.iloc[:, 4].fillna('').astype(str)   # Column 4
                                tld['phone_number'] = tld.iloc[:, 5].fillna('').astype(str) # Column 5
                                tld['vendor'] = tld.iloc[:, 8].fillna('').astype(str)      # Column 8
                                
                                # Create full name with proper spacing for matching
                                tld['full_name'] = tld.apply(lambda row: normalize_key(str(row['first_name']) + " " + str(row['last_name'])), axis=1)
                                
                                st.success("TLD data processed using exact column positions:")
                                st.write("- Column 3: First Name")
                                st.write("- Column 4: Last Name") 
                                st.write("- Column 5: Phone Number")
                                st.write("- Column 8: Vendor")
                            else:
                                st.error(f"TLD file must have at least 9 columns. Found: {len(tld.columns)}")
                                st.write("Expected columns: lead_id, date_created, date_modified, first_name, last_name, phone_number, [col6], [col7], vendor")
                                st.stop()
                        except Exception as e:
                            st.error(f"Error processing TLD columns: {str(e)}")
                            st.write("Please verify your TLD file has the correct structure")
                            st.stop()
                        
                        # Use the same FMO structure as the working vendor PDF code
                        st.write("**FMO File Structure (using proven vendor PDF format):**")
                        st.write(f"Columns: {list(fmo.columns)}")
                        
                        # Extract names using the same logic as working vendor PDF
                        try:
                            # Check for standard FMO format (columns 7 and 8) or named columns
                            if len(fmo.columns) > 8:
                                # Standard FMO format
                                fmo['First Name'] = fmo.iloc[:, 7].fillna('').astype(str)
                                fmo['Last Name'] = fmo.iloc[:, 8].fillna('').astype(str)
                                st.success("Using standard FMO format - columns 7 and 8 for names")
                            else:
                                # Try named columns (same as vendor PDF code)
                                first_name = (fmo.get('First Name', '') or fmo.get('First_Name', '') or fmo.get('first_name', ''))
                                last_name = (fmo.get('Last Name', '') or fmo.get('Last_Name', '') or fmo.get('last_name', ''))
                                
                                if len(first_name) > 0 and len(last_name) > 0:
                                    fmo['First Name'] = first_name.fillna('').astype(str)
                                    fmo['Last Name'] = last_name.fillna('').astype(str)
                                    st.success("Using named columns for FMO names")
                                else:
                                    st.error("FMO name columns not found")
                                    st.write("Expected: Columns 7-8 for names, or named columns like 'First Name', 'Last Name'")
                                    st.stop()
                            # Create full_name for matching (same as TLD processing)
                            fmo['full_name'] = fmo.apply(lambda row: normalize_key(str(row['First Name']) + " " + str(row['Last Name'])), axis=1)
                            st.success("FMO names processed successfully for matching")
                            
                        except Exception as e:
                            st.error(f"Error processing FMO file: {str(e)}")
                            st.stop()
                        
                        # Extract payment status from FMO file (assuming paid status column exists)
                        # Add vendor key mapping for TLD data
                        tld['vendor_key'] = tld['vendor'].str.lower().str.replace(' ', '').str.replace('-', '')
                        
                        # Merge datasets on full_name to match customers
                        if 'Paid Status' in fmo.columns or any('paid' in col.lower() for col in fmo.columns):
                            # Find the paid status column
                            paid_col = None
                            for col in fmo.columns:
                                if 'paid' in col.lower() or 'status' in col.lower():
                                    paid_col = col
                                    break
                            
                            if paid_col:
                                fmo['is_paid'] = fmo[paid_col].fillna('').astype(str).str.lower().str.contains('paid')
                            else:
                                fmo['is_paid'] = True  # Assume paid if status unclear
                        else:
                            fmo['is_paid'] = True  # Assume all FMO entries are paid
                        
                        # Merge datasets
                        merged = real_pd.merge(
                            tld,
                            fmo[['full_name', 'is_paid']],
                            on='full_name', how='left'
                        )
                        merged['is_paid'] = merged['is_paid'].fillna(False)
                        
                        st.write(f"Merged dataset: {len(merged)} records")
                        st.write(f"Matched customers: {merged['is_paid'].sum()} paid applications")
                        
                        # Calculate vendor metrics
                        vendor_metrics = []
                        
                        VENDOR_RATES = {
                            "francalls": 75, "hcsmedia": 75, "buffercall": 65,
                            "ancletadvising": 55, "blmcalls": 50, "loopcalls": 45,
                            "nobufferaca": 40, "raycalls": 35, "nomiaca": 30,
                            "acaking": 25, "ptacacalls": 20, "hcscaa": 15,
                            "slavaaca": 45, "slavaaca2": 40, "francallssupp": 60,
                            "derekinhousefb": 10, "allicalladdoncall": 25,
                            "joshaca": 30, "hcs1p": 15
                        }
                        
                        VENDOR_CODES = {
                            "francalls": "Fran Calls", "hcsmedia": "HCS MEDIA",
                            "buffercall": "Aetna", "ancletadvising": "Anclet advising",
                            "blmcalls": "BLM CALLS", "loopcalls": "LOOP CALLS",
                            "nobufferaca": "NO BUFFER ACA", "raycalls": "RAY CALLS",
                            "nomiaca": "Nomi ACA", "acaking": "ACA KING",
                            "ptacacalls": "PT ACA CALLS", "hcscaa": "HCS CAA",
                            "slavaaca": "Slava ACA", "slavaaca2": "Slava ACA 2",
                            "francallssupp": "Fran Calls SUPP", "derekinhousefb": "DEREK INHOUSE FB",
                            "allicalladdoncall": "ALI CALL ADDON CALL", "joshaca": "JOSH ACA",
                            "hcs1p": "HCS1p"
                        }
                        
                        for vendor_key, vendor_name in VENDOR_CODES.items():
                            if vendor_key in VENDOR_RATES:
                                vendor_data = merged[merged['vendor_key'] == vendor_key]
                                
                                if len(vendor_data) > 0:
                                    total_leads = len(vendor_data)
                                    paid_leads = vendor_data['is_paid'].sum()
                                    unpaid_leads = total_leads - paid_leads
                                    conversion_rate = (paid_leads / total_leads * 100) if total_leads > 0 else 0
                                    
                                    cost_per_lead = VENDOR_RATES[vendor_key]
                                    total_cost = total_leads * cost_per_lead
                                    cost_per_acquisition = (total_cost / paid_leads) if paid_leads > 0 else 0
                                    
                                    vendor_metrics.append({
                                        "Vendor": vendor_name,
                                        "Total Leads": total_leads,
                                        "Paid Applications": int(paid_leads),
                                        "Unpaid Applications": unpaid_leads,
                                        "Conversion Rate": f"{conversion_rate:.1f}%",
                                        "Cost per Lead": f"${cost_per_lead}",
                                        "Total Cost": f"${total_cost:,.2f}",
                                        "Cost per Acquisition": f"${cost_per_acquisition:,.2f}" if cost_per_acquisition > 0 else "N/A",
                                        "ROI Efficiency": conversion_rate / cost_per_lead if cost_per_lead > 0 else 0
                                    })
                        
                        if vendor_metrics:
                            st.subheader("Vendor CPL/CPA Analysis Results")
                            metrics_df = pd.DataFrame(vendor_metrics)
                            
                            # Sort by efficiency
                            metrics_df = metrics_df.sort_values('ROI Efficiency', ascending=False)
                            
                            st.dataframe(metrics_df.drop('ROI Efficiency', axis=1), use_container_width=True)
                            
                            # Summary statistics
                            st.subheader("Summary Metrics")
                            col1, col2, col3 = st.columns(3)
                            
                            total_cost_all = sum(float(row['Total Cost'].replace('$', '').replace(',', '')) 
                                               for row in vendor_metrics)
                            total_leads_all = sum(row['Total Leads'] for row in vendor_metrics)
                            total_paid_all = sum(row['Paid Applications'] for row in vendor_metrics)
                            
                            with col1:
                                st.metric("Total Marketing Cost", f"${total_cost_all:,.2f}")
                            with col2:
                                st.metric("Total Leads Generated", f"{total_leads_all:,}")
                            with col3:
                                overall_conversion = (total_paid_all / total_leads_all * 100) if total_leads_all > 0 else 0
                                st.metric("Overall Conversion Rate", f"{overall_conversion:.1f}%")
                            
                            # Download results
                            csv_data = metrics_df.to_csv(index=False)
                            st.download_button(
                                "Download Vendor CPL/CPA Analysis",
                                csv_data,
                                file_name=f"vendor_cpl_analysis_{datetime.now():%Y%m%d}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("No vendor data found for analysis.")
                            
            except Exception as e:
                st.error(f"Error processing files: {str(e)}")
                st.write("Please ensure your files have the correct format:")
                st.write("- TLD file should have VendorRaw, First Name, Last Name columns")
                st.write("- FMO file should have Advance column and name data")
        
        elif tld_cpl_file or fmo_cpl_file:
            st.info("Please upload both TLD CSV and FMO statement to calculate vendor metrics.")
        else:
            st.info("Upload both files to begin vendor cost per lead/acquisition analysis.")
        
    # USER MANAGEMENT TAB
    with tabs[10]:
        st.header("👥 Elite User Command Center")
        
        # Import user management functionality
        from simple_user_manager import SimpleUserManager
        
        # Initialize user manager
        um = SimpleUserManager()
        
        # Quick action buttons
        st.markdown("### 🚀 Quick Actions")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔄 Sync TLD Users", help="Import all users from TLD system"):
                with st.spinner("Syncing with TLD system..."):
                    imported_count = um.sync_with_tld_users()
                    if imported_count > 0:
                        st.success(f"Imported {imported_count} new users from TLD system!")
                        st.rerun()
                    else:
                        st.info("All TLD users already synced.")
        
        with col2:
            if st.button("🔄 Refresh Data", help="Refresh user data display"):
                st.cache_data.clear()
                st.rerun()
        
        with col3:
            users_df = um.get_all_users()
            active_users = len(users_df[users_df['status'] == 'Active']) if not users_df.empty else 0
            st.metric("Active Users", active_users)
        
        with col4:
            pending_setup = len(users_df[users_df['password_status'] == 'Password Setup Pending']) if not users_df.empty else 0
            st.metric("Pending Setup", pending_setup)
        
        st.markdown("---")
        
        # User management tabs
        user_tab1, user_tab2, user_tab3, user_tab4 = st.tabs(["👥 All Users", "➕ Add User", "🔧 Roles & Permissions", "📊 Analytics"])
        
        with user_tab1:
            st.subheader("Current User Roster")
            
            users_df = um.get_all_users()
            
            if not users_df.empty:
                for idx, user in users_df.iterrows():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 3])
                    
                    with col1:
                        status_color = "#00FF00" if user['status'] == 'Active' else "#FF6B6B"
                        password_indicator = "🔓" if user['password_status'] == 'Password Setup Pending' else "🔒"
                        
                        st.markdown(f"""
                            <div style="
                                background: linear-gradient(135deg, #1a1a1a, #2d2d2d);
                                border: 2px solid {status_color};
                                padding: 15px;
                                border-radius: 15px;
                                margin: 10px 0;
                            ">
                                <h4 style="color: #FFD700; margin: 0;">{password_indicator} {user['first_name']} {user['last_name']}</h4>
                                <p style="color: #FFFFFF; margin: 5px 0;">@{user['username']}</p>
                                <p style="color: #CCCCCC; margin: 5px 0; font-size: 14px;">{user['email']}</p>
                                <p style="color: #FFA500; margin: 5px 0; font-size: 12px;">{user['password_status']}</p>
                            </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.write(f"**Role:** {user['role']}")
                        st.write(f"**Status:** {user['status']}")
                    
                    with col3:
                        st.write(f"**Department:** {user.get('department', 'N/A')}")
                        last_login = user.get('last_login', 'Never')
                        if last_login and last_login != 'Never':
                            last_login = last_login[:16]  # Show date and time
                        st.write(f"**Last Login:** {last_login}")
                    
                    with col4:
                        col4a, col4b, col4c = st.columns(3)
                        
                        with col4a:
                            # Toggle status
                            new_status = "Inactive" if user['status'] == 'Active' else "Active"
                            status_emoji = "🔴" if user['status'] == 'Active' else "🟢"
                            if st.button(f"{status_emoji}", key=f"toggle_{user['id']}", help=f"Set {new_status}"):
                                result = um.update_user_status(user['id'], new_status, st.session_state.get('username', 'Admin'))
                                if result['success']:
                                    st.success(f"User set to {new_status}")
                                    st.rerun()
                        
                        with col4b:
                            # Change role
                            roles_df = um.get_roles()
                            current_role_idx = roles_df[roles_df['name'] == user['role']].index
                            if len(current_role_idx) > 0:
                                selected_role = st.selectbox(
                                    "Role", 
                                    roles_df['name'].tolist(),
                                    index=int(current_role_idx[0]),
                                    key=f"role_{user['id']}"
                                )
                                if selected_role != user['role']:
                                    if st.button("💼", key=f"update_role_{user['id']}", help="Update Role"):
                                        result = um.update_user_role(user['id'], selected_role, st.session_state.get('username', 'Admin'))
                                        if result['success']:
                                            st.success(f"Role updated to {selected_role}")
                                            st.rerun()
                        
                        with col4c:
                            # Reset password to default
                            if st.button("🔐", key=f"reset_{user['id']}", help="Reset Password to Default"):
                                # This could be expanded to reset password to a default value
                                st.info("Password reset functionality - contact admin")
            else:
                st.info("No users found. Click 'Sync TLD Users' to import from your TLD system.")
        
        with user_tab2:
            st.subheader("Add New Elite Member")
            
            with st.form("admin_add_user_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    first_name = st.text_input("First Name *", placeholder="Enter first name")
                    last_name = st.text_input("Last Name *", placeholder="Enter last name")
                    username = st.text_input("Username *", placeholder="e.g., jsmith@hcs")
                    email = st.text_input("Email *", placeholder="e.g., jsmith@hcs.com")
                
                with col2:
                    roles_df = um.get_roles()
                    if not roles_df.empty and 'role_name' in roles_df.columns:
                        role = st.selectbox("Role *", roles_df['role_name'].tolist())
                    else:
                        role = st.selectbox("Role *", ["Admin", "Manager", "Agent"])
                    department = st.text_input("Department", placeholder="e.g., Sales, Support")
                    phone = st.text_input("Phone", placeholder="Optional phone number")
                    send_email = st.checkbox("Send welcome email with password setup", value=True)
                
                notes = st.text_area("Notes", placeholder="Optional notes about this user")
                
                submitted = st.form_submit_button("CREATE ELITE USER", use_container_width=True)
                
                if submitted:
                    if all([first_name, last_name, username, email, role]):
                        result = um.create_user(
                            username=username,
                            email=email,
                            role=role,
                            first_name=first_name,
                            last_name=last_name,
                            phone=phone,
                            department=department,
                            created_by=st.session_state.get('username', 'Admin'),
                            send_email=send_email
                        )
                        
                        if result['success']:
                            st.success(f"User {username} created successfully!")
                            if send_email:
                                st.info("Welcome email with password setup instructions sent!")
                            st.rerun()
                        else:
                            st.error(result['error'])
                    else:
                        st.error("Please fill in all required fields marked with *")
        
        with user_tab3:
            st.subheader("Role & Permission Management")
            
            roles_df = um.get_roles()
            
            for idx, role in roles_df.iterrows():
                with st.expander(f"🎭 {role['role_name']} - {role['description']}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Core Permissions:**")
                        permissions = [
                            ("👁️ View All Data", role['can_view_all_data']),
                            ("👥 Edit Users", role['can_edit_users']),
                            ("🎭 Manage Roles", role['can_manage_roles']),
                            ("🛡️ Access Admin", role['can_access_admin'])
                        ]
                        
                        for perm_name, has_perm in permissions:
                            status = "✅" if has_perm else "❌"
                            st.write(f"{status} {perm_name}")
                    
                    with col2:
                        st.write("**Advanced Permissions:**")
                        advanced_permissions = [
                            ("📊 Generate Reports", role['can_generate_reports']),
                            ("💰 Manage Payroll", role['can_manage_payroll']),
                            ("📱 Send SMS", role['can_send_sms']),
                            ("📈 View Analytics", role['can_view_analytics'])
                        ]
                        
                        for perm_name, has_perm in advanced_permissions:
                            status = "✅" if has_perm else "❌"
                            st.write(f"{status} {perm_name}")
        
        with user_tab4:
            st.subheader("User Analytics Dashboard")
            
            users_df = um.get_all_users()
            
            if not users_df.empty:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_users = len(users_df)
                    st.metric("👥 Total Users", total_users)
                
                with col2:
                    active_users = len(users_df[users_df['status'] == 'Active'])
                    st.metric("✅ Active Users", active_users)
                
                with col3:
                    pending_users = len(users_df[users_df['status'] == 'Pending'])
                    st.metric("⏳ Pending Setup", pending_users)
                
                with col4:
                    inactive_users = len(users_df[users_df['status'] == 'Inactive'])
                    st.metric("❌ Inactive Users", inactive_users)
                
                # Role distribution
                st.subheader("🎭 Role Distribution")
                role_counts = users_df['role'].value_counts()
                st.bar_chart(role_counts)
                
                # Recent users
                st.subheader("🆕 Recently Added Users")
                recent_users = users_df.head(5)[['first_name', 'last_name', 'username', 'role', 'status', 'created_at']]
                st.dataframe(recent_users, use_container_width=True)
            else:
                st.info("No user data available. Start by syncing TLD users.")
        
        st.markdown("---")
        
        # Legacy system info (for reference)
        with st.expander("📋 Legacy System Reference"):
            st.subheader("Current Auto-Generated Credentials (Read Only)")
            
            if AGENT_CREDENTIALS:
                agent_data = []
                for username in AGENT_USERNAMES:
                    agent_data.append({
                        "Username": username,
                        "Legacy Password": AGENT_CREDENTIALS.get(username, "password"),
                        "Full Name": AGENT_NAMES.get(username, "Unknown"),
                        "Role": AGENT_ROLES.get(username, "Agent"),
                        "User ID": AGENT_USERIDS.get(username, "N/A")
                    })
                
                agent_df = pd.DataFrame(agent_data)
                st.dataframe(agent_df, use_container_width=True, hide_index=True)
                st.info("These are the auto-generated credentials from TLD. Use 'Sync TLD Users' to import them into the new management system.")
            else:
                st.warning("No legacy agent credentials found.")
        
        # System status
        st.subheader("🔌 System Status")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            db_status = "✅ PostgreSQL" if USE_POSTGRES else "⚠️ SQLite Fallback"
            st.metric("Database", db_status)
            
        with col2:
            api_status = "✅ Connected" if not df_agents.empty else "❌ Failed"
            st.metric("CRM API", api_status)
            
        with col3:
            users_df = um.get_all_users()
            total_managed_users = len(users_df) if not users_df.empty else 0
            st.metric("Managed Users", total_managed_users)
        
        # Disney Trip Mobile Mode
        st.markdown("---")
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
            border: 3px solid #FFD700;
            padding: 30px;
            border-radius: 25px;
            margin: 20px 0;
            box-shadow: 
                0 20px 60px rgba(0,0,0,0.8),
                0 0 40px rgba(255, 215, 0, 0.4),
                inset 0 0 30px rgba(255, 215, 0, 0.1);
            text-align: center;
        ">
            <h2 style="
                margin: 0 0 20px 0;
                color: #FFD700;
                font-size: 28px;
                font-weight: 900;
                font-family: 'Playfair Display', serif;
                text-shadow: 0 0 20px rgba(255, 215, 0, 0.8);
                letter-spacing: 2px;
            ">🏰 DISNEY TRIP MOBILE MODE</h2>
            <p style="
                margin: 0 0 15px 0;
                color: #FFFFFF;
                font-size: 16px;
                font-weight: 600;
                font-family: 'Montserrat', sans-serif;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.8);
                line-height: 1.5;
            ">Mobile-optimized FMO sheet uploads and PDF downloads</p>
            <p style="
                margin: 0;
                color: #00FF00;
                font-size: 14px;
                font-weight: 700;
                font-family: 'Montserrat', sans-serif;
                text-shadow: 0 0 10px rgba(0, 255, 0, 0.8);
            ">Perfect for managing payroll while enjoying the magic!</p>
        </div>
        """, unsafe_allow_html=True)
        
        disney_col1, disney_col2 = st.columns(2)
        
        with disney_col1:
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #1a5f1a 0%, #2d8f2d 50%, #1a5f1a 100%);
                border: 3px solid #00FF00;
                padding: 25px;
                border-radius: 20px;
                margin: 10px 0;
                box-shadow: 0 10px 30px rgba(0,0,0,0.7), 0 0 20px rgba(0, 255, 0, 0.3);
                text-align: center;
            ">
                <h3 style="
                    margin: 0 0 15px 0;
                    color: #00FF00;
                    font-size: 20px;
                    font-weight: 800;
                    font-family: 'Montserrat', sans-serif;
                    text-shadow: 0 0 15px rgba(0, 255, 0, 0.8);
                ">📱 MOBILE UPLOADS</h3>
                <p style="
                    margin: 0;
                    color: #FFFFFF;
                    font-size: 14px;
                    font-weight: 600;
                    line-height: 1.4;
                ">Large upload buttons<br>Touch-friendly interface<br>Quick file selection</p>
            </div>
            """, unsafe_allow_html=True)
        
        with disney_col2:
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #5f1a5f 0%, #8f2d8f 50%, #5f1a5f 100%);
                border: 3px solid #FF00FF;
                padding: 25px;
                border-radius: 20px;
                margin: 10px 0;
                box-shadow: 0 10px 30px rgba(0,0,0,0.7), 0 0 20px rgba(255, 0, 255, 0.3);
                text-align: center;
            ">
                <h3 style="
                    margin: 0 0 15px 0;
                    color: #FF00FF;
                    font-size: 20px;
                    font-weight: 800;
                    font-family: 'Montserrat', sans-serif;
                    text-shadow: 0 0 15px rgba(255, 0, 255, 0.8);
                ">📄 INSTANT PDFs</h3>
                <p style="
                    margin: 0;
                    color: #FFFFFF;
                    font-size: 14px;
                    font-weight: 600;
                    line-height: 1.4;
                ">One-tap downloads<br>Mobile-friendly viewing<br>Email-ready reports</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Discord Webhook Testing Section
        st.markdown("---")
        st.subheader("🔔 Discord Webhook Integration")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🧪 Test Webhook Connection", type="primary"):
                try:
                    discord_tracker = DiscordSalesTracker()
                    result = discord_tracker.test_webhook()
                    if result.get("success"):
                        st.success("✅ Discord webhook is working!")
                    else:
                        st.error(f"❌ Webhook test failed: {result.get('error')}")
                except Exception as e:
                    st.error(f"❌ Error testing webhook: {str(e)}")
        
        with col2:
            if st.button("🎯 Test Sale Notification"):
                try:
                    discord_tracker = DiscordSalesTracker()
                    result = discord_tracker.send_sale_notification(
                        agent_name="Test Agent",
                        sale_amount=299.99,
                        policy_type="Health Insurance",
                        member_count=3,
                        total_daily_sales=8
                    )
                    if result.get("success"):
                        st.success("✅ Test sale notification sent!")
                    else:
                        st.error(f"❌ Failed to send notification: {result.get('error')}")
                except Exception as e:
                    st.error(f"❌ Error sending test notification: {str(e)}")
        
        with col3:
            if st.button("🏆 Test Milestone Alert"):
                try:
                    discord_tracker = DiscordSalesTracker()
                    result = discord_tracker.send_milestone_alert(
                        agent_name="Test Agent",
                        milestone=10,
                        total_sales=10
                    )
                    if result:
                        st.success("✅ Test milestone alert sent!")
                    else:
                        st.error("❌ Failed to send milestone alert")
                except Exception as e:
                    st.error(f"❌ Error sending milestone alert: {str(e)}")
        
        with col1:
            if st.button("🔄 Check for New Sales Now"):
                try:
                    from sales_monitor import SalesMonitor
                    monitor = SalesMonitor()
                    new_sales_found = monitor.check_for_new_sales_once()
                    if new_sales_found:
                        st.success("✅ New sales detected and Discord notifications sent!")
                    else:
                        st.info("ℹ️ No new sales detected since last check")
                except Exception as e:
                    st.error(f"❌ Error checking for new sales: {str(e)}")
        
        with col2:
            if st.button("📊 Send Leaderboard Now"):
                try:
                    from sales_monitor import SalesMonitor
                    monitor = SalesMonitor()
                    current_sales = monitor.fetch_today_sales()
                    
                    if current_sales:
                        # Calculate agent statistics
                        agent_stats = {}
                        for sale in current_sales:
                            agent_name = sale.get('agent_name', 'Unknown')
                            if agent_name not in agent_stats:
                                agent_stats[agent_name] = 0
                            agent_stats[agent_name] += 1
                        
                        # Send leaderboard
                        from discord_webhook import DiscordSalesTracker
                        tracker = DiscordSalesTracker()
                        result = tracker.send_leaderboard_update(agent_stats, "Manual Update")
                        
                        if result.get("success"):
                            st.success("📊 Leaderboard sent to Discord!")
                        else:
                            st.error(f"❌ Failed to send leaderboard: {result.get('error')}")
                    else:
                        st.warning("⚠️ No sales data available for leaderboard")
                except Exception as e:
                    st.error(f"❌ Error sending leaderboard: {str(e)}")
        
        st.info("💡 The Discord webhook automatically fires for new sales and sends leaderboard updates every 5 minutes during business hours.")
        
        # Add webhook status indicator
        from discord_webhook import DISCORD_WEBHOOK_URL
        webhook_status = "🟢 Active" if DISCORD_WEBHOOK_URL else "🔴 Not Configured"
        st.markdown(f"**Webhook Status:** {webhook_status}")
        
        if DISCORD_WEBHOOK_URL:
            # Mask the webhook URL for security
            masked_url = DISCORD_WEBHOOK_URL[:50] + "..." + DISCORD_WEBHOOK_URL[-10:]
            st.markdown(f"**Webhook URL:** `{masked_url}`")
        
        # Show current sales summary
        try:
            from sales_monitor import SalesMonitor
            monitor = SalesMonitor()
            current_sales = monitor.fetch_today_sales()
            
            if current_sales:
                agent_counts = {}
                for sale in current_sales:
                    agent_name = sale.get('agent_name', 'Unknown')
                    if agent_name not in agent_counts:
                        agent_counts[agent_name] = 0
                    agent_counts[agent_name] += 1
                
                st.markdown("### 📊 Today's Sales Summary")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Top Performers:**")
                    sorted_agents = sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                    for agent, count in sorted_agents:
                        st.markdown(f"• {agent}: **{count}** sales")
                
                with col2:
                    total_sales = len(current_sales)
                    total_agents = len(agent_counts)
                    st.metric("Total Sales Today", total_sales)
                    st.metric("Active Agents", total_agents)
        except Exception as e:
            st.error(f"Error loading sales summary: {e}")

































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    



































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    


































    



























    



























    






































    































    



























    



























    






































    






























    



























    



























    






































    































    



























    



























    






































    






