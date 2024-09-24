"""
This example shows a way to "step through" cqsim.
At the same time, create a separate simulation in a child that runs till the end.
"""
import time
from CqSim.Cqsim_plus import Cqsim_plus



# Create Cqsim plus instance.
cqsimp = Cqsim_plus()

# Start a single cqsim simulator.
# The object returned here is a generator object.
id = cqsimp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)

# Run it while waiting for user input at each step.
while not cqsimp.check_sim_ended(id):
    # input('Press enter to continue...')

    # Advance simulation by 1 job
    cqsimp.line_step(id)
    result = cqsimp.line_step_run_on(id)

    print(result)

    
