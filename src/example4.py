"""
This example shows a way to "step through" cqsim.
At the same time, create a separate simulation in a child that runs till the end.
"""
import time
from CqSim.Cqsim_plus import Cqsim_plus
from tqdm import trange
import random
import pandas as pd
import multiprocessing as mp
from utils import disable_print, get_elements_in_range

def exp(file, proc1, proc2):

    # Create CQSim+ instance
    cqp = Cqsim_plus()
    # Disable stdout from child processes
    cqp.disable_child_stdout = True


    # Create simulator for cluster 1
    id1 = cqp.single_cqsim(
        trace_dir = '../data/InputFiles', 
        trace_file = file, 
        proc_count=proc1)

    # Create simulator for cluster 2
    # This cluster runs 50% slower than cluster 1
    id2 = cqp.single_cqsim(
        trace_dir = '../data/InputFiles', 
        trace_file = file, 
        proc_count=proc2)
    cqp.set_job_run_scale_factor(id2, 1.5)
    cqp.set_job_walltime_scale_factor(id2, 1.5)

    sim_ids = [id1, id2]

    # Parse the job file to get some job stats
    job_ids, job_procs = cqp.get_job_data(
        trace_dir = '../data/InputFiles', 
        trace_file = file)
    
    for sim in sim_ids:
        cqp.set_max_lines(sim, len(job_ids))

    # For each job
    for i in trange(len(job_ids)):
        # print('**************')


        # Simulate both clusters to get turnarounds
        turnarounds = {}
        for id in sim_ids:
            

            # Check if the job can be run
            if job_procs[i] > cqp.sim_procs[id]:
                continue
            
            # Run the simulation for only upto the next job
            results = cqp.line_step_run_on(id)
            # results = cqp.line_step_run_on_fork_based(id)

            # Parse the results
            presults = [result.split(';') for result in results]
            df = pd.DataFrame(presults, columns = ['id', 'reqProc', 'reqProc2', 'walltime', 'run', 'wait', 'submit', 'start', 'end']) 
            df = df.astype(float)
            index_of_max_value = df['id'].idxmax()
            last_job_results = df.loc[index_of_max_value]

            # Get the turnaround of the latest job
            last_job_turnaround = last_job_results['end'] - last_job_results['submit']
            turnarounds[id] = last_job_turnaround.item()
        
        # If none of the clusters could run, skip the job
        if len(turnarounds) == 0:
            for sim_id in sim_ids:
                cqp.disable_next_job(sim_id)
                cqp.line_step(sim_id)
            continue

        # Get the cluster with the lowest turnaround
        lowest_turnaround = min(turnarounds.values())
        sims_with_lowest_turnaround = [key for key, value in turnarounds.items() if value == lowest_turnaround]
        selected_sim_id = random.choice(sims_with_lowest_turnaround)

        # Add the job to the appropriate cluster and continue main simulation
        for sim_id in sim_ids:
            if sim_id == selected_sim_id:
                cqp.enable_next_job(sim_id)            
            else:
                cqp.disable_next_job(sim_id)
            with disable_print():
                cqp.line_step(sim_id)
        
        # print('Job number:', i)
        # print('Turnarounds: ', turnarounds)
        # print('Selected cluster: ', selected_sim_id)
        # print('**************\n\n')


    # Run all the simulations until complete
    while not cqp.check_all_sim_ended(sim_ids):
        for sim_id in sim_ids:
            with disable_print():
                cqp.line_step(sim_id)

   
if __name__ == '__main__':

    exp(
        file = 'theta_2022.swf',
        proc1 = 2180,
        proc2 = 2180)

    # exp(
    #     file = 'theta_1000.swf',
    #     proc1 = 2180,
    #     proc2 = 2180)


    # exp(
    #     file = 'theta_1000.swf',
    #     proc1 = 2180,
    #     proc2 = 2180)
    
    # exp(
    #     file = 'test.swf',
    #     proc1 = 100,
    #     proc2 = 100)

   








    
