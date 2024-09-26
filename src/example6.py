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

def get_elements_in_range(data_list, index, range):
  """
  Retrieves elements from a list within a specified range around a given index.

  Args:
      data_list: The list to extract elements from.
      index: The index around which to define the range.

  Returns:
      A list of elements within the range (index - 5, index + 5), 
      or an empty list if the range is invalid.
  """

  start = max(0, index - range)  # Ensure the start index is within bounds
  end = min(len(data_list), index + range + 1)  # Ensure the end index is within bounds

  if start >= end:  # Handle cases where the range is empty
      return []

  return data_list[start:end]

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

def exp():
    cqp = Cqsim_plus()
    # Create Cqsim plus instance.
    cqp.disable_child_stdout = True

    # file = 'test.swf'
    # proc1 = 100
    # proc2 = 100

    file = 'theta_2022.swf'
    proc1 = 2180
    proc2 = 2180
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
    cqp.set_job_walltime_scale_factor(id2, 1.5)

    sim_ids = [id1, id2]

    job_ids, job_procs = cqp.get_job_data(
        trace_dir = '../data/InputFiles', 
        trace_file = file)

    for sim in sim_ids:
       cqp.set_max_lines(sim, len(job_ids))

    # results = cqp.line_step_run_on(id1)
    # print(results)
    # last_job_results = results[-1].split(';')
    # print(last_job_results)
    # last_job_turnaround = float(last_job_results[8]) - float(last_job_results[7])
    # print(last_job_turnaround)

    for i in trange(len(job_ids)):
        # print('**************')

        valid_sim_ids = []
        for id in sim_ids:
            

            # Check if the job can be run
            if job_procs[i] > cqp.sim_procs[id]:
                continue
            valid_sim_ids.append(id)    
        
        # If none of the clusters could run, skip the job
        if len(valid_sim_ids) == 0:
            for sim_id in sim_ids:
                cqp.disable_next_job(sim_id)
                with disable_print():
                    cqp.line_step(sim_id)
            continue

        selected_sim_id = random.choice(valid_sim_ids)

        for sim_id in sim_ids:
            if sim_id == selected_sim_id:
                cqp.enable_next_job(sim_id)            
            else:
                cqp.disable_next_job(sim_id)
            # print(f'Cluster {sim_id}, line coutner = {cqp.line_counters[sim_id]}')
            # print(get_elements_in_range(cqp.get_job_file_mask(sim_id), cqp.line_counters[sim_id], 2))
            with disable_print():
                cqp.line_step(sim_id)


        # print('Job number:', i)
        # print('Selected cluster: ', selected_sim_id)
        # print('**************\n\n')


    # Run all the simulations until complete
    while not cqp.check_all_sim_ended(sim_ids):
        for sim_id in sim_ids:
            with disable_print():
                cqp.line_step(sim_id)

   
exp()
# if __name__ == '__main__':
#    exp()






    
