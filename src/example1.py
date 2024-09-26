"""
This example shows a way to "step through" cqsim.
"""

import time
from CqSim.Cqsim_plus import Cqsim_plus



# Create Cqsim plus instance.
cqsimp = Cqsim_plus()

# Start a single cqsim simulator.
# Returns the id of a newly created cqsim instance
id = cqsimp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)

# Run it while waiting for user input at each step.
while not cqsimp.check_sim_ended(id):
    input('Press enter to continue...')
    cqsimp.line_step(id)
