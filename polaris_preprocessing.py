import csv
from datetime import datetime
import os

# Function to convert timestamp to Unix time
def convert_to_unix(timestamp_str):
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    dt = datetime.strptime(timestamp_str, timestamp_format)
    return int(dt.timestamp())

# Function to convert the CSV file to the desired format
def csv_to_custom_format(csv_filename, output_folder, output_filename, min_sub, start, density, speed):
    job_list = []

    # Ensure the output folder exists, if not, create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Construct the full output file path
    full_output_path = os.path.join(output_folder, output_filename)
    
    with open(csv_filename, 'r') as csv_file, open(full_output_path, 'w') as output_file:
        reader = csv.DictReader(csv_file)

        i = 1
        for row in reader:
            # Extract the necessary fields from the CSV
            job_id = int(row["COBALT_JOBID"]) + i
            i +=1
            submit = convert_to_unix(row["QUEUED_TIMESTAMP"])
            wait = (convert_to_unix(row["START_TIMESTAMP"])) - (convert_to_unix(row["QUEUED_TIMESTAMP"]))
            actual_run = (convert_to_unix(row["END_TIMESTAMP"])) - (convert_to_unix(row["START_TIMESTAMP"]))
            run = int(float(row["RUNTIME_SECONDS"]))
            used_proc = int(float(row["NODES_USED"]))
            used_ave_cpu = 0.0  # Not provided in the CSV
            used_mem = 0.0  # Not provided in the CSV
            req_proc = int(float(row["NODES_REQUESTED"]))
            req_time = (row["WALLTIME_SECONDS"])
            req_mem = 0.0  # Not provided in the CSV
            status = int(row["EXIT_STATUS"]) if row["EXIT_STATUS"].isdigit() else 0
            user_id = 0  # Not provided in the CSV
            group_id = 0  # Not provided in the CSV
            num_exe = 0  # Not provided in the CSV
            num_queue = 0  # Not provided in the CSV
            num_part = 0  # Not provided in the CSV
            num_pre = 0  # Not provided in the CSV
            think_time = 0  # Not provided in the CSV

            # Build the job info dictionary based on the provided template
            temp_info = {
                'id': job_id,
                'submit': density * (submit - min_sub) + start,
                'wait': wait,
                'actualRun': actual_run,
                'run': run * speed,
                'usedProc': used_proc,
                'usedAveCPU': used_ave_cpu,
                'usedMem': used_mem,
                'reqProc': req_proc,
                'reqTime': req_time * speed,
                'reqMem': req_mem,
                'status': status,
                'userID': user_id,
                'groupID': group_id,
                'num_exe': num_exe,
                'num_queue': num_queue,
                'num_part': num_part,
                'num_pre': num_pre,
                'thinkTime': think_time,
                'start': -1,
                'end': -1,
                'score': 0,
                'state': 0,
                'happy': -1,
                'estStart': -1
            }

            # Append to the job list
            job_list.append(temp_info)
        
        output_file.write("; UnixStartTime: 0 \n; MaxNodes: 4360\n; MaxProcs: 4360\n")
        # Output to a file
        for job in job_list:
            output_file.write((str)(job['id']) + " " +
                                (str)(job['submit'])+ " " +
                                (str)(job['wait']) + " " +
                                (str)(job['actualRun']) + " " +
                                (str)(job['reqProc']) + " " +
                                "-1 -1 " + 
                                (str)(job['usedProc']) + " " +
                                (str)(job['run']) + " " + 
                                "-1 0 -1 -1 -1 -1 -1 -1 0" + "\n")

# Usage example
csv_filename = r"C:\Users\shamb\Downloads\ANL-ALCF-DJC-POLARIS_20220809_20221231.csv\ANL-ALCF-DJC-POLARIS_20220809_20221231.csv"
output_path = r"C:\Users\shamb\Downloads\cqsimplus\data\InputFiles"
output_filename = 'polaris_2022.swf'
min_sub = 0  # Example value, adjust as needed
start = 0  # Example value, adjust as needed
density = 1  # Example value, adjust as needed
speed = 1  # Example value, adjust as needed

csv_to_custom_format(csv_filename, output_path, output_filename, min_sub, start, density, speed)