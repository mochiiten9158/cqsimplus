from cqsim_api import simulate, build_para_list

name = input("Enter name of .swf file: ")

timestamp_list = simulate(path_in = "../data/InputFiles/",
                path_out = "../data/Results/",
                path_fmt = "../data/Fmt/",
                path_debug = "../data/Debug/",
                job_trace= name + '.swf',
                node_struc= name + '.swf',
                debug_lvl=4)

print(timestamp_list)
file = open('time_stamp_'+ name +'.txt', 'w')
for element in timestamp_list:
    file.write((str)(element))
    file.write("\n")
file.close()