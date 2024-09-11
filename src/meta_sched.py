import os
import cqsim_path
import IOModule.Debug_log as Class_Debug_log
import IOModule.Output_log as Class_Output_log

import CqSim.Job_trace as Class_Job_trace
#import CqSim.Node_struc as Class_Node_struc
import CqSim.Backfill as Class_Backfill
import CqSim.Start_window as Class_Start_window
import CqSim.Basic_algorithm as Class_Basic_algorithm
import CqSim.Info_collect as Class_Info_collect
import CqSim.Cqsim_sim as Class_Cqsim_sim

import Extend.SWF.Filter_job_SWF as filter_job_ext
import Extend.SWF.Filter_node_SWF as filter_node_ext
import Extend.SWF.Node_struc_SWF as node_struc_ext

trace_name = "test"
# trace_name = "SDSC-SP2-1998-4.2-cln"


import builtins
import contextlib

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


def get_job_count():

    module_debug = Class_Debug_log.Debug_log(
        lvl=0,
        show=0,
        path= f'../data/Debug/debug_{trace_name}.log',
        log_freq=1
    )
    save_name_j = f'../data/Fmt/{trace_name}.csv'
    config_name_j = f'../data/Fmt/{trace_name}.con'
    module_filter_job = filter_job_ext.Filter_job_SWF(
        trace=f'../data/InputFiles/{trace_name}.swf', 
        save=save_name_j, 
        config=config_name_j, 
        debug=module_debug
    )
    module_filter_job.feed_job_trace()
    module_filter_job.output_job_config()
    return module_filter_job.jobNum

def get_job_ids_procs():

    module_debug = Class_Debug_log.Debug_log(
        lvl=0,
        show=0,
        path= f'../data/Debug/debug_{trace_name}.log',
        log_freq=1
    )
    save_name_j = f'../data/Fmt/{trace_name}.csv'
    config_name_j = f'../data/Fmt/{trace_name}.con'
    module_filter_job = filter_job_ext.Filter_job_SWF(
        trace=f'../data/InputFiles/{trace_name}.swf', 
        save=save_name_j, 
        config=config_name_j, 
        debug=module_debug
    )
    module_filter_job.feed_job_trace()
    module_filter_job.output_job_config()
    return module_filter_job.job_ids, module_filter_job.job_procs

def run_simulation(mask, proc_count):
    with disable_print():
        print(".................... Debug")
        module_debug = Class_Debug_log.Debug_log(
            lvl=3,
            show=2,
            path= f'../data/Debug/debug_{trace_name}.log',
            log_freq=1
        )

        print(".................... Job Filter")
        save_name_j = f'../data/Fmt/{trace_name}.csv'
        config_name_j = f'../data/Fmt/{trace_name}.con'
        module_filter_job = filter_job_ext.Filter_job_SWF(
            trace=f'../data/InputFiles/{trace_name}.swf', 
            save=save_name_j, 
            config=config_name_j, 
            debug=module_debug
        )
        module_filter_job.feed_job_trace_with_mask(mask)
        module_filter_job.output_job_config()


        print(".................... Node Filter")
        save_name_n = f'../data/Fmt/{trace_name}_node.csv'
        config_name_n = f'../data/Fmt/{trace_name}_node.con'
        module_filter_node = filter_node_ext.Filter_node_SWF(
            struc=f'../data/InputFiles/{trace_name}.swf',
            save=save_name_n, 
            config=config_name_n, 
            debug=module_debug
        )
        module_filter_node.static_node_struc(proc_count)
        module_filter_node.output_node_data()
        module_filter_node.output_node_config()

        print(".................... Job Trace")
        module_job_trace = Class_Job_trace.Job_trace(
            start=0.0,
            num=8000,
            anchor=0,
            density=1,
            read_input_freq=1000,
            debug=module_debug
        )
        module_job_trace.initial_import_job_file(save_name_j)
        module_job_trace.import_job_config(config_name_j)

        print(".................... Node Structure")
        module_node_struc = node_struc_ext.Node_struc_SWF(debug=module_debug)
        module_node_struc.import_node_file(save_name_n)
        module_node_struc.import_node_config(config_name_n)

        print(".................... Backfill")
        module_backfill = Class_Backfill.Backfill(
            mode=2,
            node_module=module_node_struc,
            debug=module_debug,
            para_list=None
        )


        print(".................... Start Window")
        module_win = Class_Start_window.Start_window(
            mode=5,
            node_module=module_node_struc,
            debug=module_debug,
            para_list=['5', '0', '0'],
            para_list_ad=None)


        print(".................... Basic Algorithm")
        module_alg = Class_Basic_algorithm.Basic_algorithm (
            element=[['w', '+', '2'],[1, 0, 1]],
            debug=module_debug,
            para_list=None
        )


        print(".................... Information Collect")
        module_info_collect = Class_Info_collect.Info_collect (
            alg_module=module_alg,debug=module_debug)

        print(".................... Output Log")
        output_sys = "../data/Results/" + trace_name + ".ult"
        output_adapt = "../data/Results/" + trace_name + ".adp"
        output_result = "../data/Results/" + trace_name + ".rst"
        module_output_log = Class_Output_log.Output_log (
            output = {
                'sys':output_sys, 
                'adapt':output_adapt, 
                'result':output_result
            },
            log_freq=1
        )

        print(".................... Cqsim Simulator")
        module_list = {
            'job':module_job_trace,
            'node':module_node_struc,
            'backfill':module_backfill,
            'win':module_win,
            'alg':module_alg,
            'info':module_info_collect,
            'output':module_output_log
        }
        module_sim = Class_Cqsim_sim.Cqsim_sim(
            module=module_list, 
            debug=module_debug, 
            monitor = 500
        )
        module_sim.cqsim_sim()
        return module_output_log.job_turnarounds

job_count = get_job_count()
job_ids, job_procs = get_job_ids_procs()
print(job_ids)
print(job_procs)
clusters = {
   32 : [0 for _ in range(job_count)],
   64 : [0 for _ in range(job_count)],
   128 : [0 for _ in range(job_count)]
}

# Read jobs one by one
for i in range(job_count):

    # Simulate the clusters and find the turnarounds
    cluster_latest_job_turnarounds = {}
    for c in clusters:

        # Check if the cluster can run the job
        if job_procs[i] > c:
           continue

        # Add the job to the cluster for simualtion
        clusters[c][i] = 1

        # Run the simulation
        turnarounds = run_simulation(clusters[c], c)

        # Remove the job after simulation
        clusters[c][i] = 0


        # Find the turnaround of the last job
        latest_job_turnaround = turnarounds[job_ids[i]]["turnaround"]
        cluster_latest_job_turnarounds[c] = latest_job_turnaround

    
    # Find the cluster with the lowest turnaround, break ties randomly
    lowest_turnaround = min(cluster_latest_job_turnarounds.values())
    clusters_with_lowest_turnaround = [key for key, value in cluster_latest_job_turnarounds.items() if value == lowest_turnaround]
    import random
    selected_cluster = min(clusters_with_lowest_turnaround)
    # selected_cluster = max(clusters_with_lowest_turnaround)
    # selected_cluster = random.choice(clusters_with_lowest_turnaround)

    # Cluster with lowest turnaround gets the job
    clusters[selected_cluster][i] = 1
    

for c in clusters:
   print(f'{c}: {clusters[c]}')
