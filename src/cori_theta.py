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


# Cori
# start 1641017042
# end 1672559856

# Theta
# start 1641021254
# end 1672445845

# start diff 4212 s 70 mins
# end diff 114011 s 31 hrs



def exp_theta(tqdm_pos, tqdm_lock):
    """
    Experiment Theta

    Simulates Theta 2022 jobs on Theta

    """
    tag = f'exp_only_theta'
    trace_dir = '../data/InputFiles'
    trace_file = 'exp_only_theta.csv'
    cluster_proc = 4360


    cqp = Cqsim_plus(tag = tag)
    

    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=True)


    sim = cqp.single_cqsim(trace_dir = trace_dir, trace_file = trace_file, proc_count= cluster_proc, parsed_trace=True)

    # Configure sims to read all jobs
    cqp.set_max_lines(sim, len(job_ids))
    cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)
    cqp.disable_debug_module(sim)


    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm.tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for _ in job_ids:
        with disable_print():
            cqp.line_step(sim, write_results=True)

        with tqdm_lock:
            bar.update(1)

    while not cqp.check_sim_ended(sim):
        with disable_print():
            cqp.line_step(sim, write_results=True)

    with tqdm_lock:
        bar.close()

    return {
        "theta" : cqp.get_job_results(sim)
    }

def exp_cori(tqdm_pos, tqdm_lock):
    """
    Experiment Cori

    Simulates Cori 2022 jobs on Cori
    """
    tag = f'exp_only_cori'
    trace_dir = '../data/InputFiles'
    trace_file = 'exp_only_cori.csv'
    cluster_proc = 9688


    cqp = Cqsim_plus(tag = tag)
    

    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=True)


    sim = cqp.single_cqsim(trace_dir = trace_dir, trace_file = trace_file, proc_count= cluster_proc, parsed_trace=True)

    # Configure sims to read all jobs
    cqp.set_max_lines(sim, len(job_ids))
    cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)


    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm.tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for _ in job_ids:
        with disable_print():
            cqp.line_step(sim, write_results=True)

        with tqdm_lock:
            bar.update(1)

    while not cqp.check_sim_ended(sim):
        with disable_print():
            cqp.line_step(sim, write_results=True)

    with tqdm_lock:
        bar.close()

    return {
        "cori" : cqp.get_job_results(sim)
    }

def exp_theta_cori_merged(tqdm_pos, tqdm_lock):
    """
    Experiment Cori Theta Merged

    Simulates Cori + Theta jobs on Cori and Theta merged system.
    """
    tag = f'exp_theta_cori_merged'
    trace_dir = '../data/InputFiles'
    trace_file = 'exp_theta_cori.csv'
    cluster_proc = 14008


    cqp = Cqsim_plus(tag = tag)
    

    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=True)


    sim = cqp.single_cqsim(trace_dir = trace_dir, trace_file = trace_file, proc_count= cluster_proc, parsed_trace=True)

    # Configure sims to read all jobs
    cqp.set_max_lines(sim, len(job_ids))
    cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)
    cqp.disable_debug_module(sim)


    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm.tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for _ in job_ids:

        with disable_print():
            cqp.line_step(sim, write_results=True)

        with tqdm_lock:
            bar.update(1)

    while not cqp.check_sim_ended(sim):
        with disable_print():
            cqp.line_step(sim, write_results=True)

    with tqdm_lock:
        bar.close()

    return {
        "theta_cori_merged" : cqp.get_job_results(sim)
    }


def exp_theta_cori_opt_turn(tqdm_pos, tqdm_lock):
    """
    Theta and Cori Metascheduled using OPT turnaround
    """
    tag = f'theta_cori_opt_turn'
    trace_dir = '../data/InputFiles'
    trace_file = 'exp_theta_cori.csv'
    cluster1_proc = 4360
    cluster2_proc = 9688


    cqp = Cqsim_plus(tag = tag)
    cqp.disable_child_stdout = True

    # Cluster 1 is Theta
    id1 = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=cluster1_proc,
        parsed_trace=True)
    

    # Cluster 2 is Cori
    id2 = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=cluster2_proc,
        parsed_trace=True)

    sims = [id1, id2]


    # Get job stats
    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=True)

    # Configure sims to read all jobs
    for sim in sims:
        cqp.set_max_lines(sim, len(job_ids))
        cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)
        cqp.disable_debug_module(sim)

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
            index_of_max_value = df['submit'].idxmax()
            last_job_results = df.loc[index_of_max_value]

            # Get the turnaround of the latest job.
            last_job_turnaround = last_job_results['end'] - last_job_results['submit']
            turnarounds[sim] = last_job_turnaround.item()

        # If none of the clusters could run, skip the job.
        if len(turnarounds) == 0:
            for sim in sims:
                cqp.disable_next_job(sim)
                with disable_print():
                    cqp.line_step(sim, write_results=True)
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
                cqp.line_step(sim, write_results=True)
        
        with tqdm_lock:
            bar.update(1)

    with tqdm_lock:
        bar.close()

    # Run all the simulations until complete.
    while not cqp.check_all_sim_ended(sims):
        for sim_id in sims:
            with disable_print():
                cqp.line_step(sim_id, write_results=True)

    return {
        "theta" : cqp.get_job_results(sims[0]),
        "cori" : cqp.get_job_results(sims[1])
    }


def create_theta_cori_traces(dest_dir, size = -1):
    from trace_utils import read_job_data_swf

    file_name_merged = f'exp_theta_cori.csv'
    file_name_theta = f'exp_only_theta.csv'
    file_name_cori = f'exp_only_cori.csv'

    # start and end range from theta trace
    start=1641021254
    end=1672445845

    # read jobs theta
    df_theta = read_job_data_swf('../data/InputFiles', 'theta_2022.swf')

    # read jobs cori
    df_cori = read_job_data_swf('../data/InputFiles', 'cori_2022.swf')

    # cori get jobs in the range
    df_cori = df_cori[(df_cori['submit'] >= start) & (df_cori['submit'] <= end)].reset_index()

    # NOTE: Every above 23912 is a cori job id
    df_cori['id'] = df_cori['id'] + len(df_theta)

    # Merge the traces
    df_cori_theta = pd.concat([df_cori, df_theta], ignore_index=True)

    # Sort by submit times
    df_cori_theta = df_cori_theta.sort_values('submit').reset_index()

    df = df_cori_theta

    # Get red of reset_index residual columns
    df = df.drop(['index', 'level_0'], axis=1)

    if not size == -1:
        df = df.head(size)

    df.to_csv(f'{dest_dir}/{file_name_merged}', sep=';', index=False, header=False)

    df_cori = df[df['id'] > 23911].reset_index()
    df_cori = df_cori.drop(['index'], axis=1)
    df_cori.to_csv(f'{dest_dir}/{file_name_cori}', sep=';', index=False, header=False)

    df_theta = df[df['id'] <= 23911].reset_index()
    df_theta = df_theta.drop(['index'], axis=1)
    df_theta.to_csv(f'{dest_dir}/{file_name_theta}', sep=';', index=False, header=False)






if __name__ == '__main__':

    create_theta_cori_traces('../data/InputFiles', )

    lock = multiprocessing.Manager().Lock()
    p = []

    p.append(multiprocessing.Process(target=exp_theta, args=(1, lock,)))
    p.append(multiprocessing.Process(target=exp_cori, args=(2, lock,)))
    p.append(multiprocessing.Process(target=exp_theta_cori_merged, args=(3, lock,)))
    p.append(multiprocessing.Process(target=exp_theta_cori_opt_turn, args=(4, lock,)))

    for proc in p:
        proc.start()
    
    for proc in p:
        proc.join()

    # import sys
    # selector = int(sys.argv[1])


    # if selector == 0:
    #     # Homogeneous
    #     p.append(multiprocessing.Process(target=exp_1, args=(1, 0.5, 1, lock,)))
    #     p.append(multiprocessing.Process(target=exp_2, args=(1, 2, lock,)))

    # if selector == 1:
    #     # Select random cluster
        # p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.5, 1, lock,)))
        # p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.5, 2, lock,)))
        # p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.5, 3, lock,)))
        # p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.5, 4, lock,)))
        # p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.5, 5, lock,)))

    # if selector == 2:
    #     # Select faster cluster 60% of time
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.6, 1, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.6, 2, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.6, 3, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.6, 4, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.6, 5, lock,)))

    # if selector == 3:
    #     # Select faster cluster 70% of time
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.7, 1, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.7, 2, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.7, 3, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.7, 4, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.7, 5, lock,)))

    # if selector == 4:
    #     # Select faster cluster 80% of time
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.10, 0.8, 1, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.15, 0.8, 2, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.20, 0.8, 3, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.25, 0.8, 4, lock,)))
    #     p.append(multiprocessing.Process(target=exp_1, args=(1.30, 0.8, 5, lock,)))

    # if selector == 5:
    #     # Select the cluster with optimal turnaround
    #     p.append(multiprocessing.Process(target=exp_2, args=(1.10, 1, lock,)))
    #     p.append(multiprocessing.Process(target=exp_2, args=(1.15, 2, lock,)))
    #     p.append(multiprocessing.Process(target=exp_2, args=(1.20, 3, lock,)))
    #     p.append(multiprocessing.Process(target=exp_2, args=(1.25, 4, lock,)))
    #     p.append(multiprocessing.Process(target=exp_2, args=(1.30, 5, lock,)))


    # for proc in p:
    #     proc.start()
    

    # for proc in p:
    #     proc.join()




