import argparse
from cqsim_api import simulate, build_para_list

parser = argparse.ArgumentParser(description="Run CQSim simulation.")
parser.add_argument('-j', '--job_trace', type=str, required=True, help="Name of the job trace file")
parser.add_argument('-n', '--node_struc', type=str, required=True, help="Name of the node structure file")

# Parse the arguments
args = parser.parse_args()

# Define file paths
job_trace_name = args.job_trace
node_struc_name = args.node_struc

timestamp_list = simulate(path_in = "../data/InputFiles/",
                path_out = "../data/Results/",
                path_fmt = "../data/Fmt/",
                path_debug = "../data/Debug/",
                job_trace= job_trace_name,
                node_struc= node_struc_name,
                debug_lvl=4)

file = open('time_stamps_'+ job_trace_name.replace('.swf', '') +'.txt', 'w')
for element in timestamp_list:
    if element is not None:
        print(element)
        file.write((str)(element))
        file.write("\n")
file.close()