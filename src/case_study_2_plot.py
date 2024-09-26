"""
Plotter Case study 2

Simulate 2 clusters using CQSim+.
    - Cluster 1 uses original runtime.
    - Cluster 2 runs by a factor of x.

Two cases:
    - The user selects the faster cluster with y% probability
    - Dynamically allocate based on shortest wait time


Plots produced:
    - Avg wait time vs Node count
    - Avg wait time vs Runtime (hrs)
    - Avg wait time vs Node hours
    - Avg wait time vs Submit time
    - 

"""


result_dir = '../data/Results'

# Exp 1 results
exp1_dir = result_dir + '/probable_user'
exp1_result_files = [
    exp1_dir + '/Results/theta_2022_0.rst',
    exp1_dir + '/Results/theta_2022_1.rst'
]

# Exp 2 results
exp2_dir = result_dir + '/optimal_turnaround'
exp2_result_files = [
    exp2_dir + '/Results/theta_2022_0.rst',
    exp2_dir + '/Results/theta_2022_1.rst'
]


