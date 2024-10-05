"""
This example shows a way to "step through" cqsim.
"""

from CqSim.Cqsim_plus import Cqsim_plus

# 2349370
# trace_dir = '../data/InputFiles'
# trace_file = 'cori_2022.swf'
# proc = 9688
# trace_dir = '../data/Results/optimal_turnaround_1.0/Fmt/'
# trace_file = 'theta_2022_0.csv'
# proc = 4320


trace_dir = '../data/InputFiles'
trace_file = 'theta_cori_100K.csv'
proc = 14008

# Create Cqsim plus instance.
cqp = Cqsim_plus(tag = 'test_theta')


# Get job stats
job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=True)


# Start a single cqsim simulator.
# Returns the id of a newly created cqsim instance
#sim = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)
sim = cqp.single_cqsim(trace_dir = trace_dir, trace_file = trace_file, proc_count= proc, parsed_trace=True)


# Configure sims to read all jobs
cqp.set_max_lines(sim, len(job_ids))
cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)


# Run it while waiting for user input at each step.
while not cqp.check_sim_ended(sim):
    cqp.line_step(sim)
