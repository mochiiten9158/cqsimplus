from trace_utils import read_job_data_swf

df = read_job_data_swf('../data/InputFiles', 'polaris_2023.swf')

filtered = df[df['req_proc'] > 552]

print(filtered)