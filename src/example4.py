"""
This example shows a way to "step through" cqsim.
At the same time, create a separate simulation in a child that runs till the end.
"""
import time
from CqSim.Cqsim_plus import Cqsim_plus
from tqdm import trange
import random
import builtins
import contextlib
import pandas as pd
import multiprocessing as mp

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


@contextlib.contextmanager
def disable_print():
  """Temporarily disables the print function."""
  original_print = builtins.print

  def disabled_print(*args, **kwargs):
    pass  # Do nothing when print is called

  builtins.print = disabled_print
  try:
    yield
  finally:
    builtins.print = original_print

@contextlib.contextmanager
def enable_print():
  """Context manager that ensures printing is enabled, even if previously disabled."""
  original_print = builtins.print
  builtins.print = original_print  # Restore original print if needed
  try:
    yield
  finally:
    pass  # No need to do anything in the finally block

# Create Cqsim plus instance.
cqp = Cqsim_plus()
cqp.disable_child_stdout = True

file = 'test.swf'
proc1 = 100
proc2 = 100

# file = 'theta_1000.swf'
# proc1 = 2180
# proc2 = 2180
# Start a single cqsim simulator.
# The object returned here is a generator object.
id1 = cqp.single_cqsim(
    trace_dir = '../data/InputFiles', 
    trace_file = file, 
    proc_count=proc1)

id2 = cqp.single_cqsim(
    trace_dir = '../data/InputFiles', 
    trace_file = file, 
    proc_count=proc2)

cqp.set_job_run_scale_factor(id2, 1.5)

sim_ids = [id1, id2]

job_ids, job_procs = cqp.get_job_data(
    trace_dir = '../data/InputFiles', 
    trace_file = file)


# results = cqp.line_step_run_on(id1)
# print(results)
# last_job_results = results[-1].split(';')
# print(last_job_results)
# last_job_turnaround = float(last_job_results[8]) - float(last_job_results[7])
# print(last_job_turnaround)

for i in trange(len(job_ids)):
    print('**************')
    turnarounds = {}
    for id in sim_ids:
        

        # Check if the job can be run
        if job_procs[i] > cqp.sim_procs[id]:
            continue
        
        # Run the simulation for only upto the next job
        results = cqp.line_step_run_on(id)

        # Parse the results
        presults = [result.split(';') for result in results]
        df = pd.DataFrame(presults, columns = ['id', 'reqProc', 'reqProc2', 'walltime', 'run', 'wait', 'submit', 'start', 'end']) 
        df = df.astype(float)

        index_of_max_value = df['id'].idxmax()
        last_job_results = df.loc[index_of_max_value]

        # print(f'CLUSTER {id}')
        # print(df)


        # Get the turnaround of the latest job
        last_job_turnaround = last_job_results['end'] - last_job_results['submit']
        turnarounds[id] = last_job_turnaround.item()
        # turnarounds[id] = turnaround
    
    # If none of the clusters could run, skip the job
    if len(turnarounds) == 0:

        # TODO: mark the mask of the next job as 0 for both clusters
        continue


    lowest_turnaround = min(turnarounds.values())
    sims_with_lowest_turnaround = [key for key, value in turnarounds.items() if value == lowest_turnaround]

    selected_sim_id = random.choice(sims_with_lowest_turnaround)

    for sim_id in sim_ids:
        # print('Cluster:', sim_id)
        if sim_id == selected_sim_id:
            cqp.enable_next_job(sim_id)            
        else:
            cqp.disable_next_job(sim_id)
        with disable_print():
            cqp.line_step(sim_id)

    print('Job number:', i)
    print('Turnarounds: ', turnarounds)
    print('Selected cluster: ', selected_sim_id)
    print('**************\n\n')


# Run all the simulations until complete
while not cqp.check_all_sim_ended(sim_ids):
    for sim_id in sim_ids:
        with disable_print():
            cqp.line_step(sim_id)

        








    
