def read_next_job(file, source):
    """
    Reads the next job from the given SWF file and returns a job dictionary.
    """
    while True:
        line = file.readline()
        if not line:  # End of file
            return None
        if line.startswith(";"):  # Skip comment lines
            continue
        fields = line.strip().split()
        if len(fields) >= 18:
            job = {
                'submit': int(fields[1]),  # Unix submit time
                'wait': int(fields[2]),    # Wait time in seconds
                'actualrun': int(fields[3]),     # Runtime in seconds
                'reqProc': int(fields[4]), # Requested processors
                'usedProc': int(fields[7]), # Used processors
                'run': int(fields[8]),
                'source': source           # Source (Theta or Polaris)
            }
            return job
        else:
            print(f"Skipping malformed line: {line.strip()}")  # Debugging info for malformed lines
        
def write_job(output_file, job, id):
    """
    Writes a job to the output file in SWF format.
    """
    output_file.write(f"{id} {job['submit']} {job['wait']} {job['actualrun']} {job['reqProc']} -1 -1 {job['usedProc']} {job['run']} {job['source']} -1 0 -1 -1 -1 -1 -1 -1 0\n")

def combine_swf_files_optimized(polaris_filename, theta_filename, output_filename):
    # Open all files for reading and writing
    with open(polaris_filename, 'r') as polaris_file, \
         open(theta_filename, 'r') as theta_file, \
         open(output_filename, 'w') as output_file:

        # Optional: Write a header or comment to the output file
        output_file.write("; UnixStartTime: 0 \n; MaxNodes: 4360\n; MaxProcs: 4360\n")

        id = 1

        # Initialize first job from each file
        polaris_job = read_next_job(polaris_file, 'Polaris')
        theta_job = read_next_job(theta_file, 'Theta')

        # Compare jobs and write the smaller one to the output file, advancing one file at a time
        while polaris_job is not None or theta_job is not None:
            if polaris_job is not None and (theta_job is None or polaris_job['submit'] <= theta_job['submit']):
                write_job(output_file, polaris_job, id)
                id += 1
                polaris_job = read_next_job(polaris_file, 'Polaris')  # Move to the next Polaris job
            elif theta_job is not None:
                write_job(output_file, theta_job, id)
                id += 1
                theta_job = read_next_job(theta_file, 'Theta')  # Move to the next Theta job

# Usage
polaris_filename = r"C:\Users\shamb\Downloads\cqsimplus\data\InputFiles\polaris_2022.txt"
theta_filename = r"C:\Users\shamb\Downloads\cqsimplus\data\InputFiles\theta_experiment.txt"
output_filename = r"C:\Users\shamb\Downloads\cqsimplus\data\InputFiles\combined_polaris_theta_optimized.swf"

combine_swf_files_optimized(polaris_filename, theta_filename, output_filename)