import os
import numpy as np
import pandas as pd
import mysql.connector
import location
import functions

def clean_data(data: pd.DataFrame, district_name: str) -> pd.DataFrame:
    """
    Cleans REDCap data and adds a source tag and unique global ID.
    """
    
    if 'akarere_utuyemo3_region' in data.columns:
        data = functions.merge_columns(data, ['akarere_utuyemo3_region',], 'Facility region')
    if 'akarere_utuyemo1_eregion' in data.columns:
        data = functions.merge_columns(data, ['akarere_utuyemo1_eregion',], 'Facility region')
    if 'akarere_utuyemo5_kregion' in data.columns:
        data = functions.merge_columns(data, ['akarere_utuyemo5_kregion',], 'Facility region')
    if 'akarere_utuyemo3_region' in data.columns:
        data = functions.merge_columns(data, ['akarere_utuyemo3_region',], 'Facility region')
        
    # ---------------- Merge Location/Facility Columns ----------------
    for col_type, target in [(location.DISTRICT_COLUMNS, "District name"), 
                             (location.SECTOR_COLUMNS, "Sector name"), 
                             (location.CELL_COLUMNS, "cell name")]:
        cols = location.get_existing_columns(data, col_type)
        if cols:
            data = functions.merge_columns(data, cols, target)
            data.drop(columns=cols, errors="ignore", inplace=True)

    facility_cols = ["facility_name_gicumbi", "facility_name_kayonza", 
                     "facility_name_nyarugenge", "facility_name_karongi"]
    existing_fac = [c for c in facility_cols if c in data.columns]
    if existing_fac:
        data = functions.merge_columns(data, existing_fac, "Facility name")
        data.drop(columns=existing_fac, errors="ignore", inplace=True)

    # ---------------- Metadata & Uniqueness ----------------
    data["source_district"] = district_name
    if "record_id" in data.columns:
        data["global_id"] = data["source_district"] + "_" + data["record_id"].astype(str)

    # ---------------- Standardize & Filter ----------------
    if "data_entry_time" in data.columns:
        data["data_entry_time"] = pd.to_datetime(data["data_entry_time"], errors="coerce").dt.date

    if "hpv_vaccination_and_hpv_screening_complete" in data.columns:
        data = data[
            data["hpv_vaccination_and_hpv_screening_complete"]
            .astype(str)
            .str.lower()
            .isin(["complete", "2"])
        ]

    for col in ["last_name", "first_name"]:
        if col in data.columns:
            data[col] = data[col].astype(str).str.strip().str.upper()

    # Added 'global_id' to the required list so it is preserved
    required_cols = [
        "global_id", "record_id", "data_entry_time", "intara_utuyemo", "District name",
        "Sector name", "cell name", "village", "Facility name",'Facility region',
        "health_facility_code", "arrival_number", "partipant_study_id",
        "last_name", "first_name", "date_of_birth", "age",
        "phone_number", "national_id", "hiv_status", 
        "is_the_participant_pregnan", "previous_hpv_vaccination", 
        "screened_for_hpv", "date_of_hpv_vaccine_dose", "lot_number_of_vaccine",
        "hpv_vaccination_and_hpv_screening_complete", "adverse_event_complete"
    ]
        
    existing_required = [c for c in required_cols if c in data.columns]
    data = data[existing_required].copy()

    # Drop any rows where partipant_study_id is missing to avoid NULL constraint issues
    if "partipant_study_id" in data.columns:
        data = data.dropna(subset=["partipant_study_id"])

    return data

def save_to_mysql(df: pd.DataFrame, table_name: str, unique_column: str):
    DB_CONFIG = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 3307)),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", "yourpassword"),
        "database": os.getenv("DB_NAME", "ncds_division"),
    }

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Dynamic Schema Creation with Unique Constraint on Study ID
        cols_sql = []
        for col, dtype in df.dtypes.items():
            if col == "data_entry_time": sql_type = "DATE"
            elif "int" in str(dtype).lower(): sql_type = "INT"
            elif "float" in str(dtype).lower(): sql_type = "FLOAT"
            elif col == unique_column: sql_type = "VARCHAR(255) PRIMARY KEY"
            elif col == "partipant_study_id": sql_type = "VARCHAR(255) UNIQUE"
            else: sql_type = "TEXT"
            
            cols_sql.append(f"`{col}` {sql_type}")

        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols_sql)})")

        # Use INSERT IGNORE to skip records if partipant_study_id or PK already exists
        df_sql = df.replace({np.nan: None})
        cols_str = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        sql = f"INSERT IGNORE INTO `{table_name}` ({cols_str}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(row) for row in df_sql.to_numpy()])
        
        conn.commit()
        print(f"✅ {table_name}: {cursor.rowcount} NEW rows synced.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error on {table_name}: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
