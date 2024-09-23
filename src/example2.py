"""
This example shows a way to "step through" cqsim.
At the same time, create a separate run on simulation until the end.
"""
import time
from CqSim.Cqsim_plus import Cqsim_plus



# Create Cqsim plus instance.
cqsimp = Cqsim_plus()

# Start a single cqsim simulator.
# The object returned here is a generator object.
id = cqsimp.single_cqsim(trace_dir = '../data/InputFiles', trace_name = 'test.swf', proc_count=100)

# Run it while waiting for user input at each step.
while not cqsimp.check_sim_ended(id):
    input('Press enter to continue...')
    cqsimp.line_step_run_on(id)
