"""
Utility functions for trace file manipulations.
"""
import os
import sys
import csv
import IOModule.Debug_log as Class_Debug_log
import Extend.SWF.Filter_job_SWF as filter_job_ext
import Extend.SWF.Filter_node_SWF as filter_node_ext
import Extend.SWF.Node_struc_SWF as node_struc_ext

def get_job_data(self, trace_dir, trace_file):
    """
    Get the job data from some trace.

    Parameters
    ----------
    trace_dir : str
        A path to the directory where the trace file is located.
    trace_file : str
        The trace file name to read.

    Returns
    -------
    job_ids : lits[int]
        List of job ids.
    job_procs : list[int]
        List of processes requested for each job.
    """
    module_debug = Class_Debug_log.Debug_log(
        lvl=0,
        show=0,
        path= f'/dev/null',
        log_freq=1
    )
    module_debug.disable()
    save_name_j = f'file'
    config_name_j = f'/dev/null'
    module_filter_job = filter_job_ext.Filter_job_SWF(
        trace=f'{trace_dir}/{trace_file}', 
        save=save_name_j, 
        config=config_name_j, 
        debug=module_debug
    )
    module_filter_job.feed_job_trace()
    module_filter_job.output_job_config()

    job_ids = module_filter_job.job_ids
    job_procs = module_filter_job.job_procs

    return job_ids, job_procs
