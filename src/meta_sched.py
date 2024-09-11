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
module_filter_job.feed_job_trace_with_mask([1,0,0,0,0,0,0,0,1])
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
module_filter_node.read_node_struc()
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
# module_node_struc = node_struct_ext_aurora.Node_struc_Aurora(debug = module_debug)
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

# -- post simulation
def calculate_turnarounds(data):
    job_events = {}

    for event in data:
        timestamp, event_code, job_id = event

        if job_id not in job_events:
            job_events[job_id] = {'submit': None, 'end': None}
        if event_code == 0:  # Submit event
            job_events[job_id]['submit'] = timestamp
        elif event_code == 2:  # End event
            job_events[job_id]['end'] = timestamp

    turnaround_times = []
    for job_id, events in job_events.items():
        turnaround_time = events['end'] - events['submit']
        turnaround_times.append((job_id, turnaround_time))

    # Print the results
    return turnaround_times

print(calculate_turnarounds(Class_Cqsim_sim.time_stamps))