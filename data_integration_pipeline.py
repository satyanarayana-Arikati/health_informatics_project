import pandas as pd
import sqlite3 # For optional database integration

# --- Configuration ---
# Adjust this path based on where you unzipped your Synthea data
DATA_PATH = "C:/Users/Satya/OneDrive/Desktop/health_informatics_project/data/synthea_output/"

# --- 1. Load Data ---
print("--- 1. Loading Data ---")
try:
    df_patients = pd.read_csv(DATA_PATH + 'patients.csv')
    df_encounters = pd.read_csv(DATA_PATH + 'encounters.csv')
    df_observations = pd.read_csv(DATA_PATH + 'observations.csv')
    print("Data loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files. Check DATA_PATH: {e}")
    exit() # Exit if files are not found

# Display basic info to understand the initial structure
print("\n--- Initial DataFrames Info ---")
print("Patients DataFrame Info:")
df_patients.info()
print("\nEncounters DataFrame Head:")
print(df_encounters.head())
print("\nObservations DataFrame Head:")
print(df_observations.head())

# --- 2. Initial Exploration & Problem Identification (Manual & Code-assisted) ---
# This step is often iterative. We'll simulate some common issues.
print("\n--- 2. Initial Exploration & Problem Identification ---")

# Problem 1: Different patient ID column names for joining
#    - patients.csv: 'Id'
#    - encounters.csv: 'PATIENT'
#    - observations.csv: 'PATIENT'

# Problem 2: Date format inconsistencies (Simulated, as Synthea usually gives ISO format)
#    We'll simulate by converting some dates to different formats and back to show the process.
#    Synthea's dates are usually in YYYY-MM-DD. Let's imagine they were mixed.
#    For example, if 'START' in encounters was sometimes 'MM/DD/YYYY' and sometimes 'YYYY-MM-DD'.
#    We'll force some conversions to demonstrate.

# Problem 3: Missing values (Synthea often has complete data, so we'll simulate a few)
# Problem 4: Inconsistent categorical values (e.g., gender 'M' vs 'Male' - Synthea uses 'M'/'F')
#    We'll simulate by changing a few values for demonstration.

# --- 3. Data Cleaning & Standardization ---
print("\n--- 3. Data Cleaning & Standardization ---")

# Standardize Patient ID columns for easier merging
# We'll rename 'Id' in df_patients to 'PATIENT' to match encounters and observations
df_patients.rename(columns={'Id': 'PATIENT'}, inplace=True)
print("Renamed 'Id' in df_patients to 'PATIENT'.")

# Select relevant columns from patients for demographics
df_patients_clean = df_patients[['PATIENT', 'BIRTHDATE', 'GENDER', 'RACE', 'ETHNICITY', 'MARITAL', 'COUNTY', 'STATE', 'CITY', 'ZIP']]

# Simulate a missing 'GENDER' value in df_patients_clean for a specific patient (e.g., the first one)
if not df_patients_clean.empty:
    df_patients_clean.loc[df_patients_clean.index[0], 'GENDER'] = None
    print(f"Simulated missing GENDER for patient {df_patients_clean.loc[df_patients_clean.index[0], 'PATIENT']}.")

# Handle missing GENDER: Fill with 'Unknown' (a common strategy for categorical)
df_patients_clean['GENDER'] = df_patients_clean['GENDER'].fillna('Unknown')
print("Filled missing GENDER values with 'Unknown'.")


# Date Standardization for Encounters (Synthea's 'START' and 'STOP' are usually clean, but we'll re-format to show the process)
df_encounters['START'] = pd.to_datetime(df_encounters['START'])
df_encounters['STOP'] = pd.to_datetime(df_encounters['STOP'])
df_encounters['ENCOUNTER_DATE'] = df_encounters['START'].dt.strftime('%Y-%m-%d') # Standardize to YYYY-MM-DD
print("Standardized encounter dates to YYYY-MM-DD.")

# Simulate an inconsistent 'CODE' in encounters (e.g., some 'CODE' values are lowercase)
# This will not directly be used for mapping, but shows a cleaning step
if not df_encounters.empty and len(df_encounters['CODE']) > 2:
    df_encounters.loc[df_encounters.index[1], 'CODE'] = df_encounters.loc[df_encounters.index[1], 'CODE']
    print(f"Simulated inconsistent CODE case for encounter {df_encounters.loc[df_encounters.index[1], 'Id']}.")
df_encounters['CODE'] = str(df_encounters['CODE']) # Standardize to uppercase
print("Standardized 'CODE' column in encounters to uppercase.")


# Date Standardization for Observations (Synthea's 'DATE' is usually clean, but we'll re-format)
df_observations['DATE'] = pd.to_datetime(df_observations['DATE'])
df_observations['OBSERVATION_DATE'] = df_observations['DATE'].dt.strftime('%Y-%m-%d') # Standardize to YYYY-MM-DD
print("Standardized observation dates to YYYY-MM-DD.")

# Simulate a missing 'VALUE' in observations for a specific observation
if not df_observations.empty:
    df_observations.loc[df_observations.index[0], 'VALUE'] = None
     # CHANGE THIS LINE to use 'CODE'
    print(f"Simulated missing VALUE for observation CODE: {df_observations['CODE'].iloc[0]} (for Patient: {df_observations['PATIENT'].iloc[0]}).")



# Handle missing 'VALUE' in observations: Impute with median for numerical, or drop
# For simplicity, we'll fill with 0 or drop. For lab values, dropping might be safer if analysis is critical.
# Let's fill numerical values with median for demonstration or 'Unknown' for non-numerical
df_observations['VALUE'] = pd.to_numeric(df_observations['VALUE'], errors='coerce') # Convert to numeric, errors become NaN
median_value = df_observations['VALUE'].median()
df_observations['VALUE'] = df_observations['VALUE'].fillna(median_value if pd.notna(median_value) else 0)
print(f"Filled missing 'VALUE' in observations with median ({median_value if pd.notna(median_value) else 'N/A'}) or 0.")


# --- 4. Data Transformation & Integration (Joining) ---
print("\n--- 4. Data Transformation & Integration ---")

# Merge df_encounters with df_patients_clean to get patient demographics for each encounter
# Using a left merge to keep all encounters and add patient info where available
df_integrated_encounters = pd.merge(
    df_encounters,
    df_patients_clean,
    on='PATIENT',
    how='left',
    suffixes=('_encounter', '_patient')
)
print("Merged Encounters with Patient Demographics.")

# Merge the integrated encounters with observations
# This is a key step: linking lab results to specific patient encounters (if possible) or just patients.
# Since observations also have 'PATIENT' ID and 'DATE', we can try to link.
# For simplicity, let's merge observations to the integrated encounters based on PATIENT.
# A full clinical data warehouse might join on Patient ID and a relevant date range.
df_unified_data = pd.merge(
    df_integrated_encounters,
    df_observations,
    on='PATIENT',
    how='left', # Use left join to keep all encounters even if no matching observations
    suffixes=('_encounter', '_observation')
)
print("Merged Observations into the unified dataset.")

# Select and reorder relevant columns for the final unified view
# Choose columns that represent a comprehensive patient record from both systems
unified_columns = [
    'PATIENT', 'GENDER', 'BIRTHDATE', 'CITY', 'STATE', 'ZIP', # From Patients
    'Id_encounter', 'START', 'STOP', 'ENCOUNTER_CLASS', 'CODE_encounter', 'DESCRIPTION_encounter', 'REASONCODE', 'REASONDESCRIPTION', # From Encounters
    'Id_observation', 'OBSERVATION_DATE', 'CODE_observation', 'DESCRIPTION_observation', 'VALUE', 'UNIT', 'TYPE' # From Observations
]

# Handle potential duplicate columns from suffixes (e.g., 'CODE') by being explicit
# We used suffixes, so the original 'CODE' from observation is 'CODE_observation'

# Filter out rows if there's no meaningful data (e.g., if a merge resulted in many NaNs if using inner join)
# For a left join, we'll have NaNs for observations if no match. That's expected.

# Rename final columns for clarity if needed (e.g., 'Id_encounter' to 'Encounter_ID')
df_unified_data.rename(columns={
    'Id_encounter': 'Encounter_ID',
    'Id_observation': 'Observation_ID',
    'CODE_encounter': 'Encounter_Code',
    'DESCRIPTION_encounter': 'Encounter_Description',
    'CODE_observation': 'Observation_Code',
    'DESCRIPTION_observation': 'Observation_Description',
    'VALUE': 'Observation_Value',
    'UNIT': 'Observation_Unit',
    'TYPE': 'Observation_Type',
    'START': 'Encounter_Start_DateTime',
    'STOP': 'Encounter_End_DateTime',
    'ENCOUNTER_CLASS': 'Encounter_Type_Class'
}, inplace=True)


# Select the final set of desired columns after renaming
final_unified_columns = [
    'PATIENT', 'GENDER', 'BIRTHDATE', 'CITY', 'STATE', 'ZIP',
    'Encounter_ID', 'Encounter_Start_DateTime', 'Encounter_End_DateTime',
    'Encounter_Type_Class', 'Encounter_Code', 'Encounter_Description',
    'REASONCODE', 'REASONDESCRIPTION', 'ENCOUNTER_DATE', # Add standardized date
    'Observation_ID', 'OBSERVATION_DATE', 'Observation_Code', 'Observation_Description',
    'Observation_Value', 'Observation_Unit', 'Observation_Type'
]

# Ensure all final columns exist, fill missing ones if necessary (e.g., from merge causing new NaNs)
for col in final_unified_columns:
    if col not in df_unified_data.columns:
        df_unified_data[col] = None # Or np.nan

df_unified_data_final = df_unified_data[final_unified_columns]


print("\n--- Unified Data Sample ---")
print(df_unified_data_final.head())
print("\nUnified Data Info:")
df_unified_data_final.info()


# --- 5. Export Unified Data ---
print("\n--- 5. Exporting Unified Data ---")
OUTPUT_PATH = '../data/' # Or wherever you want the output CSV
output_csv_file = OUTPUT_PATH + 'unified_health_data.csv'
df_unified_data_final.to_csv(output_csv_file, index=False)
print(f"Unified data exported to: {output_csv_file}")

# --- Optional: Load into SQLite Database ---
# This demonstrates basic database interaction and creating a structured table.
print("\n--- Optional: Loading into SQLite Database ---")
db_path = OUTPUT_PATH + 'health_data.db'
conn = sqlite3.connect(db_path)

try:
    df_unified_data_final.to_sql('unified_patient_encounters_labs', conn, if_exists='replace', index=False)
    print(f"Unified data loaded into SQLite database: {db_path}")
    print("Table Name: unified_patient_encounters_labs")

    # Verify by querying
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM unified_patient_encounters_labs LIMIT 5")
    rows = cursor.fetchall()
    print("\nSample data from SQLite table:")
    for row in rows:
        print(row)

except Exception as e:
    print(f"Error loading data into SQLite: {e}")
finally:
    conn.close()
    print("SQLite connection closed.")

print("\n--- Pipeline Completed ---")