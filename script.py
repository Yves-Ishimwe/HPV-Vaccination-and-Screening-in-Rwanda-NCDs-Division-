import time
import io
import requests
import pandas as pd
from cleaning import clean_data, clean_data, save_to_mysql

# --- Configuration ---
API_URL = "https://redcap.rbc.gov.rw/api/"

# Add new districts here as a simple list of dictionaries
PROJECTS = [
    {"name": "Gicumbi", "token": "", "table": "gicumbi_survey"},
    {"name": "Kayonza", "token": "", "table": "kayonza_survey"},
    {"name": "Nyarugenge", "token": "", "table": "nyarugenge_survey"},
    {"name": "Karongi", "token": "", "table": "karongi_survey"}
]

def fetch_data_in_batches(token, batch_size=100):
    """Fetches records from REDCap for a specific token in chunks."""
    # 1. Get all Record IDs for this specific project
    id_payload = {'token': token, 'content': 'record', 'format': 'csv', 'fields[0]': 'record_id'}
    r = requests.post(API_URL, data=id_payload)
    
    if r.status_code != 200:
        print(f"⚠️ Failed to reach API for token {token[:5]}... Status: {r.status_code}")
        return pd.DataFrame()

    all_ids = pd.read_csv(io.StringIO(r.text))['record_id'].unique().tolist()
    print(f"📦 Records Found: {len(all_ids)}")
    
    chunks = []
    for i in range(0, len(all_ids), batch_size):
        batch = all_ids[i:i + batch_size]
        print(f"⏳ Downloading batch {i//batch_size + 1}...")
        
        payload = {
            'token': token, 'content': 'record', 'format': 'csv',
            'type': 'flat', 'rawOrLabel': 'label', 'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'true'
        }
        # Add batch IDs to payload
        for idx, r_id in enumerate(batch):
            payload[f'records[{idx}]'] = r_id
            
        res = requests.post(API_URL, data=payload, timeout=120)
        chunks.append(pd.read_csv(io.StringIO(res.text)))
        
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

if __name__ == "__main__":
    while True:
        print(f"\n🌟 --- Starting Global Sync Cycle: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
        
        for project in PROJECTS:
            print(f"\n▶️ Processing: {project['name']}")
            try:
                # Fetch using the specific project token
                df_raw = fetch_data_in_batches(project['token'])
                
                if not df_raw.empty:
                    # Clean using your standardized logic
                    df_clean = clean_data(df_raw)
                    
                    # Save to the specific table
                    save_to_mysql(df_clean, project['table'], "record_id")
                else:
                    print(f"ℹ️ No data found for {project['name']}.")
                    
            except Exception as e:
                print(f"❌ Error syncing {project['name']}: {e}")

        print("\n✅ All districts processed.")
        print("⏰ Sleeping for 30 seconds before next cycle...")
        time.sleep(30)