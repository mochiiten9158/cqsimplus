import os
import sys
import time
from multiprocessing import Process, Pipe
from unique_names_generator import get_random_name
import json
from types import SimpleNamespace
import os


import IOModule.Log_print as Log_print
import CqSim.Cqsim_sim as Class_Cqsim_sim
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
__metaclass__ = type

class Cqsim_plus:
    '''
    CQsim plus

    Class for CQSim plus core features.
    '''

    def __init__(self, monitor = 500) -> None:
        self.monitor = 500
        self.sims = []
        self.line_counters=[]
        self.end_flags = []
        self.sim_names = []
        self.sim_modules = []
        self.exp_directory = f'../data/Results/exp_{get_random_name()}'
        self.traces = {}

    def check_sim_ended(self, id):
        return self.end_flags[id]
    
    def check_all_sim_ended(self, ids):
        result = True
        for id in ids:
            result = result and self.end_flags[id]
        print(result)
        return result
    
    def single_cqsim(self, trace_dir, trace_file):
        '''
        Returns the id of a single cqsim instance.

        The cqsim instance will get a fresh set of modules.
        '''
        sim_name = get_random_name()
        sim_id = len(self.sims)

        # trace_dir = '../data/InputFiles'
        output_dir = f'{self.exp_directory}/Results'
        debug_dir = f'{self.exp_directory}/Debug'
        fmt_dir = f'{self.exp_directory}/Fmt'

        for dir in [output_dir, debug_dir, fmt_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir)
        
        # trace_file = 'test.swf'
        trace_name = trace_file.split('.')[0]
        proc_count = 100

        output_sys_file = f'{trace_name}_{sim_id}.ult'
        output_adapt_file = f'{trace_name}_{sim_id}.adp'
        output_result_file = f'{trace_name}_{sim_id}.rst'


        # Debug module
        debug_log = f'{trace_name}_{sim_id}_debug.log'
        module_debug = Class_Debug_log.Debug_log(
            lvl=3,
            show=2,
            # TODO: Change this because multiple simulators might be running the same trace
            path= f'{debug_dir}/{debug_log}',
            log_freq=1
        )
        self.module_debug = module_debug



        # Filter SWF module -- If needed
        fmt_job_file = f'{trace_name}_{sim_id}.csv'
        fmt_job_config_file = f'{trace_name}_{sim_id}.con'
        fmt_node_file = f'{trace_name}_{sim_id}_node.csv'
        fmt_node_config_file = f'{trace_name}_{sim_id}_node.con'
        
        # Avoid parsing SWF traces that have been parsed before by another sim
        if f'{trace_dir}/{trace_file}' in self.traces:
            temp_sim_id = self.traces[f'{trace_dir}/{trace_file}']
            fmt_job_file = f'{trace_name}_{temp_sim_id}.csv'
            fmt_job_config_file = f'{trace_name}_{temp_sim_id}.con'
            fmt_node_file = f'{trace_name}_{temp_sim_id}_node.csv'
            fmt_node_config_file = f'{trace_name}_{temp_sim_id}_node.con'
        # Parse SWF file
        else:
            module_filter_job = filter_job_ext.Filter_job_SWF(
                trace=f'{trace_dir}/{trace_file}', 
                save=f'{fmt_dir}/{fmt_job_file}', 
                config=f'{fmt_dir}/{fmt_job_config_file}', 
                debug=module_debug
            )
            module_filter_job.feed_job_trace()
            module_filter_job.output_job_config()


            module_filter_node = filter_node_ext.Filter_node_SWF(
                struc=f'{trace_dir}/{trace_file}',
                save=f'{fmt_dir}/{fmt_node_file}', 
                config=f'{fmt_dir}/{fmt_node_config_file}', 
                debug=module_debug
            )
            module_filter_node.static_node_struc(proc_count)
            module_filter_node.output_node_data()
            module_filter_node.output_node_config()

        # Job trace module
        module_job_trace = Class_Job_trace.Job_trace(
            job_file_path=f'{fmt_dir}/{fmt_job_file}',
            debug=module_debug,
            real_start_time=0,
            virtual_start_time=0
        )
        module_job_trace.import_job_config(f'{fmt_dir}/{fmt_job_config_file}')

        # Node structure module
        module_node_struc = node_struc_ext.Node_struc_SWF(debug=module_debug)
        module_node_struc.import_node_file(f'{fmt_dir}/{fmt_node_file}')
        module_node_struc.import_node_config(f'{fmt_dir}/{fmt_node_config_file}')

        # Backfill module
        module_backfill = Class_Backfill.Backfill(
            mode=2,
            node_module=module_node_struc,
            debug=module_debug,
            para_list=None
        )

        # Start window module
        module_win = Class_Start_window.Start_window(
            mode=5,
            node_module=module_node_struc,
            debug=module_debug,
            para_list=['5', '0', '0'],
            para_list_ad=None)

        # Basic alg module
        module_alg = Class_Basic_algorithm.Basic_algorithm (
            element=[['w', '+', '2'],[1, 0, 1]],
            debug=module_debug,
            para_list=None
        )

        # Info collect module
        module_info_collect = Class_Info_collect.Info_collect (
            alg_module=module_alg,debug=module_debug)

        # Output module
        module_output_log = Class_Output_log.Output_log (
            output = {
                'sys':f'{output_dir}/{output_sys_file}', 
                'adapt':f'{output_dir}/{output_adapt_file}', 
                'result':f'{output_dir}/{output_result_file}'
            },
            log_freq=1
        )

        module_list = {
            'job':module_job_trace,
            'node':module_node_struc,
            'backfill':module_backfill,
            'win':module_win,
            'alg':module_alg,
            'info':module_info_collect,
            'output':module_output_log
        }
    
        # CQSim module
        module_sim = Class_Cqsim_sim.Cqsim_sim(
            module=module_list, 
            debug=module_debug, 
            monitor = 500
        )


        # Get the generator object
        cqsim = module_sim.cqsim_sim()



        # Book keeping
        self.sims.append(cqsim)
        self.line_counters.append(0)
        self.end_flags.append(False)
        self.sim_names.append(sim_name)
        self.sim_modules.append(module_sim)
        self.traces[f'{trace_dir}/{trace_file}'] = sim_id

        return sim_id

    
    def line_step(self, id) -> None:
        '''
        Advances the simulator with given id by one
        line in the job file.
        '''
        try:
            next(self.sims[id])
            self.line_counters[id] += 1
        except StopIteration:
            self.end_flags[id] = True


    def line_step_run_on(self, id):
        '''
        Advances the simulator with given id by one
        line in the job file. In a child process, runs the simualtion
        from here until the end withtout revealing new jobs to the
        simulator in the child.

        For now the child's stdout is directed to a file. See _line_step_run_on() helper
        for more details.
        '''

        self.line_step(id)

        p = Process(target=self._line_step_run_on_child, args=(id,))
        p.start()
        p.join()


    def _line_step_run_on_child(self, id):

        # Modify the mask so that later jobs arent reads
        job_module = self.sim_modules[id].module['job']

        job_module.update_max_lines(self.line_counters[id])

        with open(f'runon_{self.line_counters[id]-1}.txt', 'w') as sys.stdout:
            for _ in self.sims[id]:
                pass