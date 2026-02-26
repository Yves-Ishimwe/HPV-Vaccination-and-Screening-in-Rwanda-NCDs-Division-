
import numpy as np
import pandas as pd
import mysql.connector
import location
import functions

def clean_data(data: pd.DataFrame) -> pd.DataFrame:
 # ---------------- Merge Location Columns ----------------
    district_cols = location.get_existing_columns(data, location.DISTRICT_COLUMNS)
    sector_cols = location.get_existing_columns(data, location.SECTOR_COLUMNS)
    cell_cols = location.get_existing_columns(data, location.CELL_COLUMNS)

    if district_cols:
        data = functions.merge_columns(data, district_cols, "District name")

    if sector_cols:
        data = functions.merge_columns(data, sector_cols, "Sector name")

    if cell_cols:
        data = functions.merge_columns(data, cell_cols, "cell name")

    # Drop merged source columns
    data = data.drop(columns=district_cols + sector_cols + cell_cols, errors="ignore")

    # ---------------- Keep Required Columns Safely ----------------
    required_cols = [
        'record_id', 'data_entry_time', 'intara_utuyemo','District name',
       'Sector name', 'cell name', 'village',
       'Facility name', 'health_facility_code', 'arrival_number',
       'partipant_study_id', 'last_name', 'first_name', 'date_of_birth', 'age',
        'phone_number','did_the_participant_bring','if_no_please_specify', 'national_id',
       'confirm_natinal_id', 'do_ids_match',  'hiv_status', 'is_the_participant_pregnan',
       'previous_hpv_vaccination', 'is_the_participant_selecte',
       'screened_for_hpv', 'community_health_worker_or', 'telephone_number',
       'date_of_hpv_vaccine_dose', 'site_of_injection',
       'lot_number_of_vaccine', 'observations', 
       'next_appointment_for_dose2', 'kit_code_for_selected_participants',
       'upload_the_picture_of_kit', 'first_hpv_vaccination_date',
       'site_of_injection_for_elected_participant',
       'lot_number_of_vaccine_selected', 'observations_selected',
       'next_appointment_for_second_dose',
       'data_first_name', 'hpv_vaccination_and_hpv_screening_complete',
       'system_organ_class___1', 'system_organ_class___2',
       'system_organ_class___3', 'system_organ_class___4',
       'system_organ_class___5', 'system_organ_class___6',
       'system_organ_class___7', 'system_organ_class___8', 'lymphadenopathy',
       'start_date_blood_lymphatic', 'condition_status_blood_lym',
       'end_date_blood_lymphatic', 'idiopathic_thrombocytopeni',
       'start_date_idiopathic_purp', 'cond_status_idiopathic',
       'end_date_idiopathic', 'hypersensitivity', 'start_date_hypersens',
       'cond_status_hypersens', 'end_date_hypersensit',
       'anaphylactic_reactions', 'start_date_anaphylactic_rx',
       'cond_status_anaphylactic','end_date_anaphylactic', 'anaphylactoid_reactions',
       'sta_dat_anaphylacto_bronch', 'c_status_anaphylacto_bronc',
       'end_dat_anaphylacto_bronc', 'headache', 'start_date_headache',
       'condition_status_headache', 'end_date_headache', 'dizziness',
       'start_date_dizziness', 'cond_status_dizziness', 'end_date_dizziness',
       'syncope_tonic_clonic_mvnt', 'st_date_syncope_tonic_clon',
       'cond_status_syncope_tonic', 'end_dat_syncope_tonic_clon',
       'acute_disseminated_encepha', 'sta_date_acute_disseminat',
       'cond_stat_acute_disseminat', 'end_date_acute_disseminat',
       'nausea_gastrointestin', 'start_date_nausea', 'condition_status_nausea',
       'ended_date_nausea', 'vomiting', 'start_dat_vomiting',
       'cond_status_vomiting', 'end_date_vomiting', 'urticaria',
       'start_date_urticaria', 'cond_status_urticaria', 'end_date_urticaria',
       'arthralgia_myalgia', 'sta_dat_arthralgia_myalgia',
       'c_stat_arthralgia_myalgia', 'end_dat_arthralgiamyalgia',
       'inject_site_pain_swelling', 'start_date_pain_swelling',
       'cond_status_pain_swelling', 'end_date_pain_swelling',
       'pyrexia_fatigue', 'start_date_pyrexia_fatigue',
       'cond_stat_pyrexia_fatigue', 'ended_date_pyrexia_fatigue',
       'inject_sit_prurit_bruising', 'st_date_pruritus_bruising',
       'cond_stat_prurit_bruising', 'ended_date_prurit_bruising',
       'asthenia_chills_malaise', 'start_date_asthenia_chills',
       'cond_statu_asthenia_chills', 'end_date_asthenia_chills',
       'injection_site_cellulitis', 'st_dat_inj_site_cellulitis',
       'con_stat_inj_site_cellulit', 'end_dat_inj_site_celluli',
       'health_status', 'is_this_aefi_serious', 'if_aefi_is_serious_please',
       'participant_hospitalized', 'date_of_admission', 'date_of_discharge',
       'participant_died', 'date_of_death', 'adverse_event_complete',
    ]

    existing_required = [c for c in required_cols if c in data.columns]
    data = data[existing_required].copy()

    # ---------------- Standardize Names ----------------
    if 'last_name' in data.columns:
        data['last_name'] = data['last_name'].astype(str).str.upper()

    if 'first_name' in data.columns:
        data['first_name'] = data['first_name'].astype(str).str.upper()

    # ---------------- Remove Duplicates ----------------
    dedup_cols = [
        'District name', 'Sector name',
        'cell name', 'last_name',
        'first_name', 'date_of_birth'
    ]

    dedup_cols = [c for c in dedup_cols if c in data.columns]
    data = data.drop_duplicates(subset=dedup_cols).reset_index(drop=True)

    # ---------------- Keep Completed Only ----------------
    if 'hpv_vaccination_and_hpv_screening_complete' in data.columns:
        data = data[
            data['hpv_vaccination_and_hpv_screening_complete']
            .astype(str)
            .str.lower()
            == 'complete'
        ]
    return data

def save_to_mysql(df: pd.DataFrame, table_name: str, unique_column: str):
    """
    Saves data to MySQL. If schema changes, it recreates the table.
    """
    DB_CONFIG = {
        "host": "127.0.0.1", "port": 3307,
        "user": "root", "password": "yourpassword",
        "database": "ncds_division"
    }

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Step 1: Force Table Creation/Alignment
        # If record_id is missing from the DB but in DF, we drop and recreate
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            db_cols = [row[0] for row in cursor.fetchall()]
            if unique_column not in db_cols:
                print(f"🔄 Column {unique_column} missing in DB. Recreating table...")
                cursor.execute(f"DROP TABLE `{table_name}`")

        # Step 2: Create Schema
        cols_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = "INT" if "int" in str(dtype).lower() else "FLOAT" if "float" in str(dtype).lower() else "TEXT"
            pk = " PRIMARY KEY" if col == unique_column else ""
            cols_sql.append(f"`{col}` {sql_type}{pk}")

        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols_sql)})")

        # Step 3: Sync Data
        df_sql = df.replace({np.nan: None})
        columns_str = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        
        # REPLACE INTO ensures our range-IDs (1, 2, 3...) refresh their data
        sql = f"REPLACE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(x) for x in df_sql.to_numpy()])
        
        conn.commit()
        print(f"✅ Sync Successful: {len(df)} rows updated.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()