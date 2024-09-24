"""
This example creates a meta scheduling expeirment.
Shows the usage CQSim+ to manage multiple instances of cqsim.
"""
from CqSim.Cqsim_plus import Cqsim_plus


# Create Cqsim plus instance.
cqp = Cqsim_plus()

# Cluster 0 which runs on 1.5x (50% slower)
id1 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)
cqp.set_job_run_scale_factor(id1, 1.5)

# Cluster 1 which runs on 1x
id2 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)
cqp.set_job_run_scale_factor(id2, 1.0)

# Run all simulators until the end
while not cqp.check_all_sim_ended(ids=[id1, id2]):

    # Step all simulators by one line in job file
    for id in [id1, id2]:
        cqp.line_step(id)