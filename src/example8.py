"""
When runtime of the jobs is scaled using set_job_run_scale_factor() of CQSim+,
the program crashes with the following:



Traceback (most recent call last):
  File "/home/yash/git/cqsimplus/src/example8.py", line 27, in <module>
    cqp.line_step(id)
  File "/home/yash/git/cqsimplus/src/CqSim/Cqsim_plus.py", line 304, in line_step
    next(self.sims[id])
  File "/home/yash/git/cqsimplus/src/CqSim/Cqsim_sim.py", line 66, in cqsim_sim
    yield from self.scan_event()
  File "/home/yash/git/cqsimplus/src/CqSim/Cqsim_sim.py", line 205, in scan_event
    self.event_job(self.current_event['para'])
  File "/home/yash/git/cqsimplus/src/CqSim/Cqsim_sim.py", line 235, in event_job
    self.finish(self.current_event['para'][1])
  File "/home/yash/git/cqsimplus/src/CqSim/Cqsim_sim.py", line 267, in finish
    self.module['node'].node_release(job_index,self.currentTime)
  File "/home/yash/git/cqsimplus/src/Extend/SWF/Node_struc_SWF.py", line 58, in node_release
    self.job_list.pop(j)
IndexError: pop index out of range


TODO: The error does not occur when the walltime is scaled with the same factor. Find out why.

"""
from CqSim.Cqsim_plus import Cqsim_plus


# Create Cqsim plus instance.
cqp = Cqsim_plus()

# Cluster 0 which runs on 1.5x (50% slower)
id1 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)
cqp.set_job_run_scale_factor(id1, 1.5)

# TODO: Debug why walltime must also be scaled to avoid bug
cqp.set_job_walltime_scale_factor(id1, 2.0)

# Cluster 1 which runs on 1x
# id2 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'theta_1000.swf', proc_count=4360)
# cqp.set_job_run_scale_factor(id2, 1.0)

ids = [id1]

# Run all simulators until the end
while not cqp.check_all_sim_ended(ids):

    # Step all simulators by one line in job file
    for id in ids:
        cqp.line_step(id)