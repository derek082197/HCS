import streamlit as st
import requests
from datetime import date

CRM_API_URL  = "https://hcs.tldcrm.com/api/egress/policies"
CRM_API_ID   = "310"
CRM_API_KEY  = "87c08b4b-8d1b-4356-b341-c96e5f67a74a"

st.title("🕵️ CRM API Debug")

st.write("Calling CRM API…")
try:
    resp = requests.get(
        CRM_API_URL,
        headers={
            "tld-api-id":  CRM_API_ID,
            "tld-api-key": CRM_API_KEY
        },
        timeout=10
    )
    st.write("Status code:", resp.status_code)
    js = resp.json()
    st.write("Top‐level keys:", list(js.keys()))
    # the actual payload is under js["response"]
    data = js.get("response", {})
    st.write("Response keys:", list(data.keys()))

    results = data.get("results", [])
    st.write(f"Number of results in this page: {len(results)}")
    if results:
        st.json(results[0])  # show first record

    # does there appear to be a “next” link?
    nxt = data.get("navigate", {}).get("next", None)
    st.write("Navigate.next is", nxt)

    # finally, build a tiny DataFrame and show the first 5:
    import pandas as pd
    df = pd.DataFrame(results)
    st.write("Here's your DataFrame preview:")
    st.dataframe(df.head(5))

except Exception as e:
    st.error(f"Error calling CRM: {e}")
