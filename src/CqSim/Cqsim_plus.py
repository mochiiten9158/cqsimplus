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
import os
import time
from multiprocessing import Process, Pipe

__metaclass__ = type

class Cqsim_plus:

    FWA_EXPECT_REPLY = False

    # FWA Constants
    FWA_DYN_READ_JOB = 0

    # EXP Constant
    EXP_META_VER1 = 1


    sim_index = 0

    line_counter = 0


    def __init__(self, monitor = 500) -> None:
        self.module_list = None
        self.module_debug = None
        self.monitor = 500
        self.sims = []
        self.trace_dir = "../data/InputFiles/"
        # trace_name = "SDSC-SP2-1998-4.2-cln"
        # trace_name = 'ANL-Intrepid-2009-1'
        self.trace_name = "test"
        self.proc_count = 100

        self.module_list = self.initialize_modules(self.trace_name, self.trace_dir, self.proc_count)

        # For each module ad this class as a context object
        for module in self.module_list:
            self.module_list[module].context = self

    
    # TODO: Add API to manage modules and various CQSims, in parallel or serial
    def initialize_modules(self,
              trace_name, trace_dir, proc_count):
        

        # TODO: Write API to manage file IO
        output_sys = f'../data/Results/{self.trace_name}.ult'
        output_adapt = f'../data/Results/{self.trace_name}.adp'
        output_result = f'../data/Results/{self.trace_name}.rst'


        print(".................... Debug")
        module_debug = Class_Debug_log.Debug_log(
            lvl=3,
            show=2,
            path= f'../data/Debug/debug_{trace_name}.log',
            log_freq=1
        )
        self.module_debug = module_debug

        print(".................... Job Filter")
        save_name_j = f'../data/Fmt/{trace_name}.csv'
        config_name_j = f'../data/Fmt/{trace_name}.con'
        module_filter_job = filter_job_ext.Filter_job_SWF(
            trace=f'{trace_dir}{trace_name}.swf', 
            save=save_name_j, 
            config=config_name_j, 
            debug=module_debug
        )
        module_filter_job.feed_job_trace()
        module_filter_job.output_job_config()


        print(".................... Node Filter")
        save_name_n = f'../data/Fmt/{trace_name}_node.csv'
        config_name_n = f'../data/Fmt/{trace_name}_node.con'
        module_filter_node = filter_node_ext.Filter_node_SWF(
            struc=f'{trace_dir}{trace_name}.swf',
            save=save_name_n, 
            config=config_name_n, 
            debug=module_debug
        )
        module_filter_node.static_node_struc(proc_count)
        module_filter_node.output_node_data()
        module_filter_node.output_node_config()

        print(".................... Job Trace")
        module_job_trace = Class_Job_trace.Job_trace(
            job_file_path=save_name_j,
            debug=module_debug,
            real_start_time=0,
            virtual_start_time=0
        )
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
        module_output_log = Class_Output_log.Output_log (
            output = {
                'sys':output_sys, 
                'adapt':output_adapt, 
                'result':output_result
            },
            log_freq=1
        )

        print(".................... Cqsim Simulator")
        
        return {
            'job':module_job_trace,
            'node':module_node_struc,
            'backfill':module_backfill,
            'win':module_win,
            'alg':module_alg,
            'info':module_info_collect,
            'output':module_output_log
        }

    
    def start_single_cqsim(self):
        self.module_sim = Class_Cqsim_sim.Cqsim_sim(
            module=self.module_list, 
            debug=self.module_debug, 
            monitor = 500
        )
        cqsim = self.module_sim.cqsim_sim()
        self.sims.append(cqsim)
        return cqsim

    
    # TODO: later Generalize to next_step()
    def line_step(self):
        self.line_counter += 1
        return next(self.sims[self.sim_index])
    

    def line_step_run_on_child(self):
        print('CHILD START*********************************')
        

        # Modify the mask so that later jobs arent read
        job_module = self.module_list['job']

        job_module.update_max_lines(self.line_counter)


        for _ in self.sims[self.sim_index]:
            pass
        print('CHILD END*********************************')


    def line_step_run_on(self):

        self.line_step()

        p = Process(target=self.line_step_run_on_child, args=())
        p.start()
        p.join()

        return ''



    def fork_wait_advice(self, code):
        '''
        Call this function anaywhere in cqsim, it will fork and create a child
        which may carry out some computation. The parent will wait for the child
        and use it's result (piped back to parent) to advice the smiulation going
        forward.

        '''
        if code == self.FWA_DYN_READ_JOB:

            r,w = os.pipe()
            pid = os.fork()
            # Parent
            if pid > 0 :

                os.close(w) 
                self.r = os.fdopen(r) 
                # This is blocking call, so parent process will wait until child completes
                
                os.waitpid(pid,0)
                # Read the result from the pipe
                # Compare with the other cluster
                # Set the mask for the next job
                # Continue until we reach here again for the next job
                print('####CHILD CHILD CHILD')
                str = self.r.read() 
                print( "Parent reads =", str) 


            # Child
            else :
                os.close(r)
                self.FWA_EXPECT_REPLY = True 
                w = os.fdopen(w, 'w')
                self.w = w 
                # Fake the end of the file
                # The simulation runs on till then end
                # Modify Output log to write result to a file or pipe
                return -1

    def send_advice(self, message):
        if self.FWA_EXPECT_REPLY:
            self.w.write(message)
            self.w.close()