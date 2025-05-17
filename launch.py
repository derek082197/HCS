import subprocess
import webbrowser
import time

# Full absolute path to app.py
app_path = r"C:\Users\derek\Desktop\HCS AUTOMATED\app.py"

# Launch streamlit silently and log output
subprocess.Popen(f'streamlit run "{app_path}" > log.txt 2>&1', shell=True)

# Wait 2 seconds and open browser
time.sleep(2)
webbrowser.open("http://localhost:8501")




