"""
Functions for experiments.
Import these functions to run various experiments.

"""
from CqSim.Cqsim_plus import Cqsim_plus
import tqdm
from utils import probabilistic_true, disable_print
import pandas as pd
import random
import multiprocessing



def exp_theta():
    """
    Experiment Theta

    Simulates Theta 2022 jobs on Theta

    """
    pass

def exp_cori():
    """
    Experiment Cori

    Simulates Cori 2022 jobs on Cori
    """
    pass

def exp_cori_theta():
    """
    Experiment Cori Theta

    Simulates Cori + Theta jobs on Cori and Theta using optimal turnaround scheduling.
    """
    pass




def exp_1(x, y, tqdm_pos, tqdm_lock):
    """
    Experiment 1

    Cluster setup:
        Cluster 1 uses original runtime
        Cluster 2 runs at a factor of x

    Scheduling Strategy:
        The user selects the faster cluster with y% probability.

    Parameters
    ----------
    x : float
        cluster 2 speed factor
    y : float
        proability with which user selects cluster 1

    Returns
    -------
    results: dict
        result of the experiment
    """
    tag = f'probable_user_{x}_{y}'
    cqp = Cqsim_plus(tag = tag)
    cqp.disable_child_stdout = True

    trace_dir = '../data/InputFiles'
    trace_file = 'theta_2022.swf'
    cluster1_proc = 2180
    cluster2_proc = 2180

    # Cluster 1 original runtime.
    id1 = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=cluster1_proc)
    

    # Cluster 2 runs at a factor of x.
    id2 = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=cluster2_proc)
    cqp.set_job_run_scale_factor(id2, x)
    cqp.set_job_walltime_scale_factor(id2, x)

    sims = [id1, id2]

    # Get job stats
    job_ids, job_procs = cqp.get_job_data(trace_dir, trace_file)
    job_submits = cqp.get_job_submits(trace_dir, trace_file)

    # Configure sims to read all jobs
    for sim in sims:
        cqp.set_max_lines(sim, len(job_ids))
        cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)

    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm.tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)
    
    for i in range(len(job_ids)):

        valid_sims = []
        for sim in sims:
            

            # Check if the job can be run
            if job_procs[i] > cqp.sim_procs[sim]:
                continue
            valid_sims.append(sim)    
        
        # If none of the clusters could run, skip the job
        if len(valid_sims) == 0:
            for sim in sims:
                cqp.disable_next_job(sim)
                with disable_print():
                    cqp.line_step(sim)
            continue

        elif len(valid_sims) == 1:
            selected_sim = valid_sims[0]

        elif len(valid_sims) == 2:
            if probabilistic_true(y):
                # Choose cluster 1.
                selected_sim = sims[0]

            else:
                # Chose cluster 2.
                selected_sim = sims[1]


        # selected_sim = random.choice(valid_sims)

        for sim in sims:
            
            if sim == selected_sim:
                cqp.enable_next_job(sim)            
            else:
                cqp.disable_next_job(sim)
            
            with disable_print():
                cqp.line_step(sim)
        
        with tqdm_lock:
            bar.update(1)

    with tqdm_lock:
        bar.close()


    
    # Run all the simulations until complete.
    while not cqp.check_all_sim_ended(sims):
        for sim_id in sims:
            with disable_print():
                cqp.line_step(sim_id)

    return {
        "cluster 1" : cqp.get_job_results(sims[0]),
        "cluster 2" : cqp.get_job_results(sims[1])
    }

def exp_2(x, tqdm_pos, tqdm_lock):
    """
    Experiment 2

    Cluster setup:
        Cluster 1 uses original runtime
        Cluster 2 runs at a factor of x

    Scheduling Strategy:
        Always select the cluster with the lowest turnaround.
    """
    tag = f'optimal_turnaround_{x}'
    trace_dir = '../data/InputFiles'
    trace_file = 'theta_2022.swf'
    cluster1_proc = 2180
    cluster2_proc = 2180


    cqp = Cqsim_plus(tag = tag)
    cqp.disable_child_stdout = True

    # Cluster 1 original runtime.
    id1 = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=cluster1_proc)
    

    # Cluster 2 runs at a factor of x.
    id2 = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=cluster2_proc)
    cqp.set_job_run_scale_factor(id2, x)
    cqp.set_job_walltime_scale_factor(id2, x)

    sims = [id1, id2]


    # Get job stats
    job_ids, job_procs = cqp.get_job_data(trace_dir, trace_file)
    job_submits = cqp.get_job_submits(trace_dir, trace_file)

    # Configure sims to read all jobs
    for sim in sims:
        cqp.set_max_lines(sim, len(job_ids))
        cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)

    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm.tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for i in range(len(job_ids)):


        turnarounds = {}
        # First simulate the new job on both clusters.
        for sim in sims:

            # Check if the job can be run.
            if job_procs[i] > cqp.sim_procs[sim]:
                continue
            
            # Run the simulation for only upto the next job.
            results = cqp.line_step_run_on(sim)

            # Parse the results
            presults = [result.split(';') for result in results]
            df = pd.DataFrame(presults, columns = ['id', 'reqProc', 'reqProc2', 'walltime', 'run', 'wait', 'submit', 'start', 'end']) 
            df = df.astype(float)
            index_of_max_value = df['id'].idxmax()
            last_job_results = df.loc[index_of_max_value]

            # Get the turnaround of the latest job.
            last_job_turnaround = last_job_results['end'] - last_job_results['submit']
            turnarounds[sim] = last_job_turnaround.item()

        # If none of the clusters could run, skip the job.
        if len(turnarounds) == 0:
            for sim in sims:
                cqp.disable_next_job(sim)
                with disable_print():
                    cqp.line_step(sim)
            continue
        

        # Get the cluster with the lowest turnaround.
        lowest_turnaround = min(turnarounds.values())
        sims_with_lowest_turnaround = [key for key, value in turnarounds.items() if value == lowest_turnaround]
        selected_sim = random.choice(sims_with_lowest_turnaround)

        # Add the job to the appropriate cluster and continue main simulation.
        for sim in sims:
            if sim == selected_sim:
                cqp.enable_next_job(sim)            
            else:
                cqp.disable_next_job(sim)
            with disable_print():
                cqp.line_step(sim)
        
        with tqdm_lock:
            bar.update(1)

    with tqdm_lock:
        bar.close()

    # Run all the simulations until complete.
    while not cqp.check_all_sim_ended(sims):
        for sim_id in sims:
            with disable_print():
                cqp.line_step(sim_id)

    return {
        "cluster 1" : cqp.get_job_results(sims[0]),
        "cluster 2" : cqp.get_job_results(sims[1])
    }


if __name__ == '__main__':

    lock = multiprocessing.Manager().Lock()
    p = []

    import sys
    selector = int(sys.argv[1])


    if selector == 0:
        # Homogeneous
        p.append(multiprocessing.Process(target=exp_1, args=(1, 0.5, 1, lock,)))
        p.append(multiprocessing.Process(target=exp_2, args=(1, 2, lock,)))

    if selector == 1:
        # Select random cluster
        p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.5, 1, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.5, 2, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.5, 3, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.5, 4, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.5, 5, lock,)))

    if selector == 2:
        # Select faster cluster 60% of time
        p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.6, 1, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.6, 2, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.6, 3, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.6, 4, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.6, 5, lock,)))

    if selector == 3:
        # Select faster cluster 70% of time
        p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.7, 1, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.7, 2, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.7, 3, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.7, 4, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.7, 5, lock,)))

    if selector == 4:
        # Select faster cluster 80% of time
        p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.8, 1, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.8, 2, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.8, 3, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.8, 4, lock,)))
        p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.8, 5, lock,)))

    if selector == 5:
        # Select the cluster with optimal turnaround
        p.append(multiprocessing.Process(target=exp_2, args=(1.10, 1, lock,)))
        p.append(multiprocessing.Process(target=exp_2, args=(1.15, 2, lock,)))
        p.append(multiprocessing.Process(target=exp_2, args=(1.20, 3, lock,)))
        p.append(multiprocessing.Process(target=exp_2, args=(1.25, 4, lock,)))
        p.append(multiprocessing.Process(target=exp_2, args=(1.30, 5, lock,)))


    for proc in p:
        proc.start()
    

    for proc in p:
        proc.join()



