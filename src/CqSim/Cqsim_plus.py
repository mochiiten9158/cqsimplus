import IOModule.Log_print as Log_print
import CqSim.Cqsim_sim as Class_Cqsim_sim
import os

__metaclass__ = type

class Cqsim_plus:

    FWA_EXPECT_REPLY = False

    # FWA Constants
    FWA_DYN_READ_JOB = 0

    # EXP Constant
    EXP_META_VER1 = 1



    def __init__(self, module_list, module_debug = None, monitor = None) -> None:
        self.module_list = module_list
        self.module_debug = module_debug


        # For each module ad this class as a context object
        for module in module_list:
            module_list[module].context = self

    
    def run_simulation_vanilla(self):
        self.module_sim = Class_Cqsim_sim.Cqsim_sim(
            module=self.module_list, 
            debug=self.module_debug, 
            monitor = 500
        )
        generator = self.module_sim.cqsim_sim()
        self.send_advice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')



    def run_experiment(
            self,
            clusters,
            cluster_speeds):
        '''
        Run the meta scheduling experiement.
        '''
        pass

    
    def linestep():
        pass



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