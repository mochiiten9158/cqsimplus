from CqSim.Cqsim_plus import Cqsim_plus
import random
random.seed(10)

cqp = Cqsim_plus()
file = 'theta_1000.swf'
proc = 4360
# Start a single cqsim simulator.
# The object returned here is a generator object.
id = cqp.single_cqsim(
    trace_dir = '../data/InputFiles', 
    trace_file = file, 
    proc_count=proc)

job_ids, job_procs = cqp.get_job_data(
    trace_dir = '../data/InputFiles', 
    trace_file = file)
print(job_ids)
mask = [random.randint(0, 1) for _ in job_ids]

cqp.set_mask(id, mask)

while not cqp.check_sim_ended(id):
    cqp.line_step(id)


# Validate if the jobs were skipped

# Actual jobs run
results = cqp.get_results(id)
actual = []
for result in results:
    parsed = result.split(';')
    id = int(parsed[0])
    actual.append(id)

# Expected jobs run
expected = []
for i in range(len(job_ids)):
    if mask[i] == 1:
        expected.append(job_ids[i])

from collections import Counter

def assert_same_elements(list1, list2):
  assert Counter(list1) == Counter(list2), f"Lists {list1} and {list2} do not have the same elements."

assert_same_elements(actual, expected)


