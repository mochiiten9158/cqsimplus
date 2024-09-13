import argparse
def calculate_averages(filename):
    """
    Calculates average wait/run, wait/proc, wait/(run*proc), and wait from a file with job entries.

    Args:
        filename: The name of the file to process.

    Returns:
        A tuple containing:
            - Average wait/run across all jobs.
            - Average wait/proc across all jobs.
            - Average wait/(run*proc) across all jobs
            - Average wait across all jobs
    """
    total_wait_run_ratio = 0
    total_wait_proc_ratio = 0
    total_wait_run_proc_ratio = 0
    total_wait = 0
    job_count = 0

    with open(filename, 'r') as file:
        for line in file:
        
            if line[0] == ';':
                continue
            fields = line.strip().split(';')
            if len(fields) != 9:
                continue

            job_id, _, proc, run, wait, _, _, _, _ = fields
            proc, run, wait = float(proc), float(run), float(wait)

            if run == 0 or proc == 0:
                continue

            total_wait_run_ratio += wait / run
            total_wait_proc_ratio += wait / proc
            total_wait_run_proc_ratio += wait / (run * proc)
            total_wait += wait

            job_count += 1

    if job_count == 0:
        return 0, 0, 0, 0

    average_wait_run = total_wait_run_ratio / job_count
    average_wait_proc = total_wait_proc_ratio / job_count
    average_wait_run_proc = total_wait_run_proc_ratio / job_count
    average_wait = total_wait / job_count

    return average_wait_run, average_wait_proc, average_wait_run_proc, average_wait, job_count



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate average wait/run and wait/proc from a job file.", allow_abbrev=False)
    parser.add_argument("filename", help="The name of the file to process.")
    args, _ = parser.parse_known_args()

    average_wait_run, average_wait_proc, average_wait_run_proc, average_wait, job_count = calculate_averages(args.filename)
    print(f'Jobs Processed: {job_count}')
    print(f"Average wait/run: {average_wait_run}")
    print(f"Average wait/proc: {average_wait_proc}")
    print(f"Average wait/(run*proc): {average_wait_run_proc}")
    print(f"Average wait: {average_wait}")
