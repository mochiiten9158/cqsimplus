"""
This example shows a way to "step through" cqsim.
"""

from CqSim.Cqsim_plus import Cqsim_plus

trace_dir = '../data/InputFiles'
trace_file = 'theta_1000.swf'


# Create Cqsim plus instance.
cqp = Cqsim_plus(tag = 'test_theta_1000')


# Get job stats
job_ids, job_procs = cqp.get_job_data(trace_dir, trace_file)
job_submits = cqp.get_job_submits(trace_dir, trace_file)


# Start a single cqsim simulator.
# Returns the id of a newly created cqsim instance
sim = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)


# Configure sims to read all jobs
cqp.set_max_lines(sim, len(job_ids))
cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)


# Run it while waiting for user input at each step.
while not cqp.check_sim_ended(sim):
    cqp.line_step(sim)
