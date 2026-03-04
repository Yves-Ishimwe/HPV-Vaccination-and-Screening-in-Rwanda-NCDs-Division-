import time
import io
import requests
import pandas as pd
from clean import clean_data, save_to_mysql

API_URL = "https://redcap.rbc.gov.rw/api/"
COMBINED_TABLE = "all_districts_hpv_master"

PROJECTS = [
    {"name": "Gicumbi", "token": "   ", "table": "gicumbi_survey"},
    {"name": "Kayonza", "token": "   ", "table": "kayonza_survey"},
    {"name": "Nyarugenge", "token": "   ", "table": "nyarugenge_survey"},
    {"name": "Karongi", "token": "   ", "table": "karongi_survey"}
]

def fetch_data(token, batch_size=100):
    try:
        # 1. Get IDs
        id_payload = {'token': token, 'content': 'record', 'format': 'csv', 'fields[0]': 'record_id'}
        r = requests.post(API_URL, data=id_payload, timeout=30)
        if r.status_code != 200: return pd.DataFrame()
        
        all_ids = pd.read_csv(io.StringIO(r.text))['record_id'].unique().tolist()
        chunks = []
        
        # 2. Batch Fetch
        for i in range(0, len(all_ids), batch_size):
            batch = all_ids[i:i + batch_size]
            payload = {
                'token': token, 'content': 'record', 'format': 'csv',
                'type': 'flat', 'rawOrLabel': 'label', 'rawOrLabelHeaders': 'raw',
                'exportCheckboxLabel': 'true'
            }
            for idx, r_id in enumerate(batch):
                payload[f'records[{idx}]'] = r_id
                
            res = requests.post(API_URL, data=payload, timeout=120)
            chunks.append(pd.read_csv(io.StringIO(res.text)))
            
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Fetch Error: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    while True:
        print(f"\n🚀 Cycle Started: {time.strftime('%H:%M:%S')}")
        master_list = []

        for project in PROJECTS:
            print(f"--- {project['name']} ---")
            df_raw = fetch_data(project['token'])
            
            if not df_raw.empty:
                # Pass project name to clean_data for the source tag
                df_clean = clean_data(df_raw, project['name'])
                
                # Save to District Table
                save_to_mysql(df_clean, project['table'], "record_id")
                
                # Collect for Master Table
                master_list.append(df_clean)
            else:
                print("No data found.")

        # Sync the Fifth (Master) Table
        if master_list:
            print("\n--- Syncing Master Consolidated Table ---")
            df_master = pd.concat(master_list, ignore_index=True)
            # Use global_id for the master table to prevent ID overwrites
            save_to_mysql(df_master, COMBINED_TABLE, "global_id")

        print("\n✅ Cycle Complete. Sleeping 5 minutes...")
        time.sleep(300)
