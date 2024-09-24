"""
This example shows a way to "step through" cqsim.
"""

import time
from CqSim.Cqsim_plus import Cqsim_plus



# Create Cqsim plus instance.
cqsimp = Cqsim_plus()

# Start a single cqsim simulator.
# Returns the id of a newly created cqsim instance
id = cqsimp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'test.swf', proc_count=100)

# Run it while waiting for user input at each step.
cqsimp.disable_next_job(id)
cqsimp.line_step(id)
print(cqsimp.line_step_run_on(id))
input('Press enter to continue...')

for _ in cqsimp.sims[id]:
    pass


    