import logging
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import sys
import tempfile
import random
from time import sleep
from pymeasure.log import console_log
from pymeasure.display.Qt import QtWidgets
from pymeasure.display.windows import ManagedWindow
from pymeasure.experiment import Procedure, Results
from pymeasure.experiment import IntegerParameter, FloatParameter, Parameter
from CqSim.Cqsim_plus import Cqsim_plus
from utils import disable_print
import pandas as pd

class Theta(Procedure):


    # iterations = IntegerParameter('Loop Iterations', default=100)
    DATA_COLUMNS = ['Iteration', 'Random Number']

    def __init__(self, **kwargs):
        self.sim = None
        self.cqp = None
        self.job_ids = None
        self.job_procs = None
        self.job_submits = None
        self.trace_dir = '../data/InputFiles'
        self.trace_file = 'only_theta.csv'
        self.proc = 4360
        self.cqp = Cqsim_plus(tag = 'gui_only_cori')
        self.job_ids, self.job_procs, self.job_submits = self.cqp.get_job_data(self.trace_dir, self.trace_file, parsed_trace=True)
        super().__init__(**kwargs)

    def startup(self):

        
        self.sim = self.cqp.single_cqsim(trace_dir = self.trace_dir, trace_file = self.trace_file, proc_count= self.proc, parsed_trace=True)
        log.info("Created cqsim instance..")

        self.cqp.set_max_lines(self.sim, len(self.job_ids))
        self.cqp.set_sim_times(self.sim, real_start_time=self.job_submits[0], virtual_start_time=0)
        self.cqp.disable_child_stdout = True

    def execute(self):
        i = 0
        for job_id in self.job_ids:

            results = self.cqp.line_step_run_on(self.sim)
            presults = [result.split(';') for result in results]
            df = pd.DataFrame(presults, columns = ['id', 'reqProc', 'reqProc2', 'walltime', 'run', 'wait', 'submit', 'start', 'end']) 
            df = df.astype(float)
            index_of_max_value = df['id'].idxmax()
            last_job_results = df.loc[index_of_max_value]
            wait_time = last_job_results['wait']
            data = {
                'Iteration': job_id,
                'Random Number': wait_time
            }
            self.emit('results', data)
            log.debug("Emitting results: %s" % data)
            self.emit('progress', 100 * i / len(self.job_ids))
            i = i + 1
            with disable_print():
                self.cqp.line_step(self.sim)

class MainWindow(ManagedWindow):

    def __init__(self):
        super().__init__(
            procedure_class=Cori,
            x_axis='Iteration',
            y_axis='Random Number'
        )
        self.setWindowTitle('CqSim+')   






# class RandomProcedure(Procedure):

#     iterations = IntegerParameter('Loop Iterations', default=100)
#     delay = FloatParameter('Delay Time', units='s', default=0.2)
#     seed = Parameter('Random Seed', default='12345')

#     DATA_COLUMNS = ['Iteration', 'Random Number']

#     def startup(self):
#         log.info("Setting the seed of the random number generator")
#         random.seed(self.seed)

#     def execute(self):
#         log.info("Starting the loop of %d iterations" % self.iterations)
#         for i in range(self.iterations):
#             data = {
#                 'Iteration': i,
#                 'Random Number': random.random()
#             }
#             self.emit('results', data)
#             log.debug("Emitting results: %s" % data)
#             self.emit('progress', 100 * i / self.iterations)
#             sleep(self.delay)
#             if self.should_stop():
#                 log.warning("Caught the stop flag in the procedure")
#                 break


# class MainWindow(ManagedWindow):

#     def __init__(self):
#         super().__init__(
#             procedure_class=RandomProcedure,
#             inputs=['iterations', 'delay', 'seed'],
#             displays=['iterations', 'delay', 'seed'],
#             x_axis='Iteration',
#             y_axis='Random Number'
#         )
#         self.setWindowTitle('GUI Example')





if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())