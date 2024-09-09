from __future__ import annotations
import argparse
from jinja2 import Template
import os
import sys
from datetime import datetime
import time
import re
import cqsim_path
import cqsim_main
import urwid
import subprocess
import threading
import time

def range_type(arg):
    try:
        start, end = map(int, arg.split('-'))
        if start > end:
            raise argparse.ArgumentTypeError("Invalid range: start value is \
                                             greater than end value")
        return start, end
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid range: use format start-end")

def parse_args():
    """
    Parse the args for generating the workload traces.
    
    NOTE: Default args set as described in paper mentioned in file header 
    comment.The paper creates 15 unique workload traces. Each trace is composed 
    of 60 jobs in the order of their arrival time. In all the traces, the 
    inter-arrival duration of 200 seconds between consecutive job. 
    """

    print("----Args----")
    parser = argparse.ArgumentParser(description="Argument Parser")

    parser.add_argument("-nt", "--number_of_traces", type=int, default=15,
                        help="Number of threads (default: 15)")
    parser.add_argument("-nj", "--number_of_jobs", type=int, default=60,
                        help="Number of jobs (default: 60)")
    parser.add_argument("-ad", "--arrival_delta", type=int, default=200,
                        help="Additional value (default: 200)")
    parser.add_argument("-C", "--working_directory", type=str, default=".",
                        help="Path to traces (default: '.')")
    parser.add_argument("-nr", "--node_range", type=range_type, default=(1, 10),
                        help='Job resource node count range (default: 1-10)')
    parser.add_argument("-ppnr", "--ppn_range", type=range_type, 
                        default=(1, 4), 
                        help='Job resource ppn range (default: 1-4)')
    parser.add_argument("-ompr", "--omp_range", type=range_type, 
                        default=(1, 10), 
                        help='Job resource Open MP range (default: 1-10)')
    # TODO: Creating shared jobs on particular vnodes

    args = parser.parse_args()
    print("Number of traces:", args.number_of_traces)
    print("Number of jobs:", args.number_of_jobs)
    print("Arrival delta:", args.arrival_delta)
    print("Node range:", args.node_range)
    print("PPN range:", args.ppn_range)
    print("Num OMP thread range:", args.omp_range)
    print("Working directory:", args.working_directory)
    print("----Args----")
    return args

def generate(
        num_traces, 
        jobs_per_trace, 
        arrival_delta, 
        gen_path,
        node_range,
        ppn_range,
        omp_range
    ):
    """
    Function to generate the job traces
    """


    """
    Randomly select the range for possible node configurations for each job 
    while ensuring that its maximum node count does not exceed the system size.
    """
    node_counts = [i for i in range(*node_range)]

    """
    Randomly chooses the range of OpenMP thread count
    """
    ppn_counts = [i for i in range(*ppn_range)]

    """
    Randomly chooses the range of PPN count
    """
    omp_thread_counts = [i for i in range(*omp_range)]

    """
    The job workloads are Multi-zone versions of NPB (NPB-MZ) that are designed 
    to exploit multiple levels of parallelism in applications and to test the 
    effectiveness of multi-level and hybrid parallelization paradigms and tools. 

    Randomly select the problem type and class.

    The paper uses a sampling range for the problem size between class C and 
    class E with a memory footprint between 0.8 and 250 GB for the above 
    problems. This script does the same but between class S and class D.

    TODO: Test class E and F
    """
    problems = ['bt-mz','lu-mz','sp-mz']
    problem_classes = ['S', 'W', 'A', 'B', 'C', 'D']

    """
    For the walltime the paper estimates the job execution time on an 
    intermediate node count (geometric mean of the maximum and minimum nodes 
    requested by the job).(TODO: what does this mean??)

    TODO: what walltime to use for each job? for now randomly select between 
    0.5, 1 and 1.5 hrs. Some jobs might fail to complete in the specified time 
    due to this.
    """
    walltimes = ['00:30:00', '01:00:00', '01:30:00']

    """
    Set the path for the location of the benchmark executables.
    """
    benchmark_path = '/pbsusers/NPB3.4.2-MZ/NPB3.4-MZ-MPI/bin'
    
    job_template = """
    #!/bin/bash
    #PBS -l nodes={{ nodes }}:ppn={{ ppn }}
    #PBS -l walltime={{ walltime }}
    #PBS -q {{ queue_name }}
    #PBS -o {{ output_file }}
    #PBS -e {{ error_file }}

    cd $PBS_O_WORKDIR
    export P4_RSHCOMMAND=/opt/pbs/bin/pbs_tmrsh
    export OMP_NUM_THREADS={{ OMP_NUM_THREADS }}

    mpirun {{executable_path}}
    """
    template = Template(job_template)

    # Generate traces where each trace is a list of job scripts.
    traces = []

    # TODO: Generate submit script for the jobs
    traces_job_paths = []
    for i in range(0 ,num_traces):
        job_scripts = []
        job_paths = []
        shell_submit_script = []

        import time
        trace_dir = f'{gen_path}/trace_{i}'
        shell_submit_script_path = f'{trace_dir}/submit_trace.sh'


        # TODO: Generate an ansible script
        ansible_script = ''
        ansible_script_path = f'{gen_path}/trace/submit_trace.yml'

        for j in range(0 ,jobs_per_trace):
            import random
            _nodes = random.choice(node_counts)
            _ppn = random.choice(ppn_counts)
            _omp_thread_count = random.choice(omp_thread_counts)
            _walltime = random.choice(walltimes)
            _out_file = f'{_nodes}_{_ppn}_{_walltime.replace(":", "_")}.out'
            _err_file = f'{_nodes}_{_ppn}_{_walltime.replace(":", "_")}.err'
            _problem = random.choice(problems)
            _pclass = random.choice(problem_classes)
            _exec_path = f'{benchmark_path}/{_problem}.{_pclass}.x'
            import time
            _job_dir = f'{trace_dir}/job_{j}'
            _job_file = f'{_job_dir}/runme.pbs'
            job_paths.append(_job_file)

            # TODO: Implement submission as some user (requires ansible)
            _job_user = ''


            parameters = {
                'nodes': _nodes,
                'ppn': _ppn,
                'walltime': _walltime,
                'queue_name': 'workq',
                'output_file': _out_file,
                'error_file': _err_file,
                'OMP_NUM_THREADS' : _omp_thread_count,
                'executable_path': _exec_path
            }
            _job_script = template.render(**parameters)
            job_scripts.append(_job_script)
            shell_submit_script.append(f'cd {_job_dir} && qsub {_job_file} && \
                                       sleep {arrival_delta} && cd ..\n')
            os.makedirs(_job_dir, exist_ok=True)
            with open(_job_file, 'w') as f:
                f.write(_job_script)
        

        traces.append(job_scripts)
        traces_job_paths.append(job_paths)
        with open(shell_submit_script_path, 'w') as f:
            f.writelines(shell_submit_script)
    return traces_job_paths


def main():
    from UI import RootController
    RootController().main()


if __name__ == "__main__":
    main()
    exit(0)

    args = parse_args()

    # First Generate the required traces
    traces_job_paths = generate(
        args.number_of_traces,
        args.number_of_jobs,
        args.arrival_delta,
        args.working_directory,
        args.node_range,
        args.ppn_range,
        args.omp_range
    )
    for trace in traces_job_paths:
        print('--- Submitting Trace ---')
        for job in trace:
            print('\t' + job)
        print('------------------------')


    # Psuedo code for streaming setting
    # for trace in traces:
    #   submit(trace)
    #   swf_file = []     
    #   while (pbs queue empty)
    #       # blocking call
    #       complete_swf, inclomplete_swf = get_next_swf()
    #       simulate(complete_swf, incomplete_swf)
