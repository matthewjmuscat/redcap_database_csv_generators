import os
import pandas as pd
import logging
import datetime

# Get timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the base directory and log file path
#base_dir = r'/home/mjm/Documents/UBC/Research/nextgenbrachy/patient data/Prostate Patients (Matt 2022-2020)'  # Set your base directory path here
base_dir = r'/home/mjm/Documents/UBC/Research/nextgenbrachy/patient data/Prostate Patients (Dakota 2022-2020)'  # Set your base directory path here

log_filename = os.path.join(base_dir, f'redcap_processing_{timestamp}.log')


# Set up logging to print to console and save to the log file in base_dir
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='w'),  # Log to a file in base_dir
        logging.StreamHandler()  # Log to the console
    ]
)

# Define the headers matching REDCap fields
headers = [
    'record_id', 'redcap_event_name', 'redcap_repeat_instrument', 'redcap_repeat_instance',
    'ctv_d999', 'ctv_d99', 'ctv_d90', 'ctv_v100', 'ctv_v150', 'ctv_v200',
    'ptv_d90', 'ptv_d99', 'ptv_v100', 'ptv_v150', 'ptv_v200', 
    'rect_d2cc', 'rect_d1cc', 'rect_d01cc', 'rect_v50', 'rect_v80', 'rect_v100',
    'uret_d01cc', 'uret_d1cc', 'uret_d5cc', 'blad_d1cc', 'blad_d2cm3', 'blad_v50', 
    'blad_v80', 'blad_v100', 'dose_parameters_tg43_complete', 
    'ctv_d999_v2', 'ctv_d99_v2', 'ctv_d90_v2', 'ctv_v100_v2', 'ctv_v150_v2', 
    'ctv_v200_v2', 'ptv_d90_v2', 'ptv_d99_v2', 'ptv_v100_v2', 'ptv_v150_v2', 
    'ptv_v200_v2', 'rect_d2cc_v2', 'rect_d1cc_v2', 'rect_d01cc_v2', 'rect_v50_v2', 
    'rect_v80_v2', 'rect_v100_v2', 'uret_d01cc_v2', 'uret_d1cc_v2', 'uret_d5cc_v2', 
    'blad_d1cc_v2', 'blad_d2cm3_v2', 'blad_v50_v2', 'blad_v80_v2', 'blad_v100_v2', 
    'dose_parameters_mc_complete'
]

# Define mappings for each file type within TG43 and TG186

# TG43 mappings
metrics_to_redcap_tg43_prostate = {
    'D99.9 (%) / Gy': 'ctv_d999',
    'D99 (%) / Gy': 'ctv_d99',
    'D90 (%) / Gy': 'ctv_d90',
    'V100 / %': 'ctv_v100',
    'V150 / %': 'ctv_v150',
    'V200 / %': 'ctv_v200'
}

metrics_to_redcap_tg43_rectum = {
    'D2 (cc) / Gy': 'rect_d2cc',
    'D1 (cc) / Gy': 'rect_d1cc',
    'D0.1 (cc) / Gy': 'rect_d01cc',
    'V50 / %': 'rect_v50',
    'V80 / %': 'rect_v80',
    'V100 / %': 'rect_v100'
}

metrics_to_redcap_tg43_urethra = {
    'D0.1 (cc) / Gy': 'uret_d01cc',
    'D1 (cc) / Gy': 'uret_d1cc', 
    'D5 (cc) / Gy': 'uret_d5cc'
}

metrics_to_redcap_tg43_bladder = {
    'D1 (cc) / Gy': 'blad_d1cc',
    'D2 (cc) / Gy': 'blad_d2cm3',
    'V50 / %': 'blad_v50',
    'V80 / %': 'blad_v80',
    'V100 / %': 'blad_v100'
}

# TG186 mappings (suffix '_v2' added to each field name)
metrics_to_redcap_tg186_prostate = {k: f"{v}_v2" for k, v in metrics_to_redcap_tg43_prostate.items()}
metrics_to_redcap_tg186_rectum = {k: f"{v}_v2" for k, v in metrics_to_redcap_tg43_rectum.items()}
metrics_to_redcap_tg186_urethra = {k: f"{v}_v2" for k, v in metrics_to_redcap_tg43_urethra.items()}
metrics_to_redcap_tg186_bladder = {k: f"{v}_v2" for k, v in metrics_to_redcap_tg43_bladder.items()}

# Dictionary to map file structures to their corresponding mapping dictionaries
tg43_mappings = {
    'prostate': metrics_to_redcap_tg43_prostate,
    'rectum': metrics_to_redcap_tg43_rectum,
    'urethra': metrics_to_redcap_tg43_urethra,
    'bladder': metrics_to_redcap_tg43_bladder
}

tg186_mappings = {
    'prostate': metrics_to_redcap_tg186_prostate,
    'rectum': metrics_to_redcap_tg186_rectum,
    'urethra': metrics_to_redcap_tg186_urethra,
    'bladder': metrics_to_redcap_tg186_bladder
}

# Initialize an empty DataFrame with the headers
data = pd.DataFrame(columns=headers)

# Function to read metrics from a CSV file based on the structure type
def read_metrics_file(file_path, metrics_mapping, nan_handling):
    metrics_data = pd.read_csv(
    file_path,
    skiprows=2,
    header=None,
    usecols=[0, 1],
    na_values=['n/a'],  # Specify 'n/a' as a missing value
    keep_default_na=True  # Keep default missing value detection (e.g., empty strings, NaN)
    )

    # Replace NaN values specifically created from 'n/a' with 0
    if nan_handling == 1:
        metrics_data.fillna(0, inplace=True)
    elif nan_handling == 2:
        pass

    metrics_dict = {}
    
    # Iterate over rows to find values that map to REDCap fields
    for index, row in metrics_data.iterrows():
        metric_name, value = row[0], row[1]
        if metric_name in metrics_mapping:
            redcap_field = metrics_mapping[metric_name]
            metrics_dict[redcap_field] = value

    return metrics_dict




# Function to check completeness based on the selected option
def check_completeness(patient_data, mappings, completion_option):
    if completion_option == 1:
        # Option 1: Mark everything as complete
        return "2"
    elif completion_option == 2:
        # Option 2: Mark as incomplete if any required field is missing
        for mapping_dict in mappings.values():  # Iterate over structure mappings
            for field in mapping_dict.values():
                if field not in patient_data or pd.isna(patient_data[field]) or patient_data[field] == "":
                    return "1"  # Incomplete if any field is missing
        return "2"  # Complete if all fields are present
    elif completion_option == 3:
        # Option 3: Mark as incomplete only if no fields are complete
        structures_present = any(
            any(patient_data.get(field, "") not in [None, "", "0"]  # Check if any field has a valid value
                for field in mapping_dict.values())
            for mapping_dict in mappings.values()
        )
        return "2" if structures_present else "1"  # Complete if any field is valid
    else:
        raise ValueError("Invalid completion option selected.")

# Modify the `process_directory` function to use the new completeness logic
def process_directory(base_dir, min_id=None, max_id=None, ids_to_skip = [],completion_option = 1, nan_handling = 1):
    for patient_folder in os.listdir(base_dir):
        patient_path = os.path.join(base_dir, patient_folder)

        if os.path.isdir(patient_path) and patient_folder.startswith("PT") and patient_folder[2:6].isdigit():
            record_id = int(patient_folder[2:6])
            if record_id in ids_to_skip:
                logging.info(f"Skipping {patient_folder} (record_id {record_id}): set to skip.")
                continue

            if (min_id is not None and record_id < min_id) or (max_id is not None and record_id > max_id):
                logging.info(f"Skipping {patient_folder} (record_id {record_id}): out of range ({min_id}-{max_id}).")
                continue

            logging.info(f"Processing {patient_folder} with record_id {record_id}.")
            # Initialize patient data
            patient_data = {
                'record_id': f"{record_id_prefix}{record_id}",
                'redcap_event_name': event_name,
                'redcap_repeat_instrument': '',  # Leave blank if not using repeating instruments
                'redcap_repeat_instance': ''     # Leave blank if not using repeating instances
            }

            tg43_path = os.path.join(base_dir, patient_folder, 'TG43')
            tg186_path = os.path.join(base_dir, patient_folder, 'TG186')

            for path, mappings, complete_field in [
                (tg43_path, tg43_mappings, 'dose_parameters_tg43_complete'),
                (tg186_path, tg186_mappings, 'dose_parameters_mc_complete')
            ]:
                if os.path.exists(path) and os.listdir(path):  # Check if folder exists and is not empty
                    logging.info(f"Processing folder: {path}")
                    for file_name in os.listdir(path):
                        if file_name.endswith("_metrics.csv"):
                            structure = None
                            if "bladder" in file_name.lower():
                                structure = 'bladder'
                            elif "rectum" in file_name.lower():
                                structure = 'rectum'
                            elif "prostate" in file_name.lower():
                                structure = 'prostate'
                            elif "urethra" in file_name.lower():
                                structure = 'urethra'

                            if structure:
                                file_path = os.path.join(path, file_name)
                                logging.info(f"Processing file: {file_path} for structure: {structure}")
                                metrics_dict = read_metrics_file(file_path, mappings[structure], nan_handling)
                                patient_data.update(metrics_dict)

                    # Check completeness based on the selected option
                    patient_data[complete_field] = check_completeness(patient_data, mappings, completion_option)
                    logging.info(f"{complete_field} set to {patient_data[complete_field]} for {patient_folder}.")
                else:
                    logging.warning(f"Skipping {path}: folder is missing or empty.")
            
            data.loc[len(data)] = patient_data

# Define how to handle n/a values in _metrics.csv from egs_brachy
# Default is 1!
# 1: set n/a to 0 (this is actually appropriate because n/a in egs_brachy output for a particular metric I think actually means 0)
# 2: set n/a to nan
nan_handling = 1

# Define the completion option
# Default is 1!
# 1: Mark everything as complete
# 2: Mark as incomplete if any required field is missing
# 3: Mark as incomplete only if no fields are complete for the TG folder
completion_option = 3  # Change this to 1, 2, or 3 based on desired behavior


# Execute the directory processing
min_id, max_id = 1, 93
record_id_prefix = ""
event_name = 'baseline_arm_1'  # Replace this with the correct event name for your project

# Identify patient folders to skip
#ids_to_skip = [158] # 2022-2020 (Matt) cohort
ids_to_skip = [30,53,64,87,88] # 2022-2020 (Dakota) cohort

process_directory(base_dir, min_id=min_id, max_id=max_id, ids_to_skip = ids_to_skip, completion_option = completion_option)


# Replace NaN with blank in the DataFrame before saving to CSV
data.fillna('', inplace=True)

# Sort the DataFrame by 'record_id' (assuming 'record_id' can be sorted as an integer)
data['record_id'] = pd.to_numeric(data['record_id'], errors='coerce')  # Ensure numeric sorting
data.sort_values(by='record_id', inplace=True)

# Save to CSV in the same directory as the patient folders


file_name = f'redcap_data_upload_Matthew_{min_id}-{max_id}_SK-{ids_to_skip}- {timestamp} - complete opt_{completion_option}.csv'

filepath = os.path.join(base_dir, file_name)
data.to_csv(filepath, index=False)
logging.info(f"CSV file saved to {filepath}")
logging.info(f"Log file saved to {log_filename}")
