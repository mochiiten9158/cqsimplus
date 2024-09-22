"""
This example creates a meta scheduling expeirment.
Shows the usage CQSim+ to manage multiple instances of cqsim.
"""
from CqSim.Cqsim_plus import Cqsim_plus


# Create Cqsim plus instance.
cqp = Cqsim_plus()

id1 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'test.swf')
id2 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'test.swf')
id3 = cqp.single_cqsim(trace_dir = '../data/InputFiles', trace_file = 'test3.swf')

# Run all simulators until the end
while not cqp.check_all_sim_ended(ids=[id1, id2, id3]):

    # Step all simulators by one line in job file
    for id in [id1, id2, id3]:
        cqp.line_step(id)