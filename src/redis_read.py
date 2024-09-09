import redis
import json
import os
import copy
qstat = {}
qstat_copy = {}
complete_swfs = {}
partial_swfs = {}
partial_swfs2 = {}
swf_header = ''
location = '/pbsusers/log.swf'
event_no = -1
def get_event_no():
    global event_no
    event_no = event_no + 1
    return event_no


def process_stream_entry(data):
    redis_id = data[0]
    timestamp_ms = int(redis_id.split('-')[0])
    timestamp_s = int(timestamp_ms/1000)
    event_type = data[1]['event_type']
    json_data = json.loads(data[1]['json_data'])
    id = int(data[1]['job_id'].split('.')[1])
    if event_type == 'q':
        # Parameters to record
        submit = timestamp_s
        reqProc = json_data['reqProc']
        reqTime = json_data['reqTime']
        reqMem = -1
        # reqMem = json_data['']
        qstat[id] = {
            'id':id,
            'submit': submit,
            'reqProc': reqProc,
            'reqTime': reqTime,
            'reqMem': reqMem,
        }
        print('q: ', end=':')
        write_swf_json(qstat[id])
        # print('###### Running Parital CQSIM #####')
        run_cq_sim(qstat, name = f'{get_event_no()}_queue')
        # print('###### Running Parital CQSIM #####')
        # print(json_data)
    elif event_type == 'r':
        # Parameters to record
        # wait = timestamp_s - submit
        qstat[id]['wait'] = timestamp_s - qstat[id]['submit']
        print('r: ', end=':')
        write_swf_json(qstat[id])
        run_cq_sim(qstat, name = f'{get_event_no()}_run')
        # print(json_data)
    elif event_type == 'mom_r':
        # Parameters to record
        # none
        # print(json_data)
        pass
    elif event_type == 'mom_e':
        # Parameters to record
        # run = timestamp_s - submit - wait
        # usedProc
        # usedAveCPU
        # usedMem
        # status
        if 'run' not in qstat[id]:
            qstat[id]['run'] = timestamp_s - qstat[id]['submit'] - qstat[id]['wait']
            qstat[id]['status'] = json_data['status']
            print('e: ', end=':')
            write_swf_json(qstat[id])
            complete_swfs[id] = qstat[id]
            run_cq_sim(complete_swfs, name = f'{get_event_no()}_end')
        # print(json_data)
        pass
    else:
        # print('!!unknown')
        # print(json_data)
        pass
    pass

def write_swf_json(j):
    print(json.dumps(j))

def get_swf(allowPartial = False):
    '''
    from the queue of data reported by the hook, create 
    input for cqsim.
    '''
    pass

def dict_get(dict, key):
    if key in dict:
        return dict[key]
    else:
        return -1


def run_cq_sim(data, PartialData = False, name = 'test'):
    '''
    run cqsim given data
    '''
    global swf_header
    filename = 'streamdata.swf'
    filepath = f'../data/InputFiles/{filename}'
    filecontent = swf_header
    job_ids = list(data.keys())
    job_ids.sort()
    alt_index = 0
    for job_id in job_ids:
        _swf_row = [
            alt_index,
            dict_get(data[job_id], 'submit'),
            dict_get(data[job_id], 'wait'),
            dict_get(data[job_id], 'run'),
            dict_get(data[job_id], 'reqProc'),
            -1,
            -1,
            dict_get(data[job_id], 'reqProc'),
            dict_get(data[job_id], 'reqTime'),
            dict_get(data[job_id], 'reqMem'),
            dict_get(data[job_id], 'status'),
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            0
        ]
        line = ''
        for i in _swf_row:
            line = line + ' ' + str(i)
        filecontent = filecontent + line + '\n'
        alt_index = alt_index + 1
    print('--------SWF---------')
    print(filecontent)
    print('--------------------')
    with open(filepath, 'w') as file:
            file.write(filecontent)  # Write content to the file
    
    from cqsim_api import simulate
    try:
        simulate(
            path_in = "../data/InputFiles/",
            path_out = "../data/Results/",
            path_fmt = "../data/Fmt/",
            path_debug = "../data/Debug/",
            job_trace= filename,
            node_struc= filename,
            debug_lvl=4,
            output = name,
            debug = f'debug_{name}'
        )
    except:
        print('[!!!!!]Could not run cqsim')


def parse_args():
    """
    Parse the args.
    """
    global swf_header
    import argparse
    parser = argparse.ArgumentParser(description="Argument Parser")

    parser.add_argument("-s", "--redis_stream", type=str, default="redis-hook",
                        help="Redis stream name (default: 15)")

    parser.add_argument("-n", "--max_nodes", type=int, default=15,
                        help="Maximum node count (default: 15)")
    
    parser.add_argument("-p", "--max_procs", type=int, default=60,
                        help="Maximum procs (default: 60)")

    args = parser.parse_args()
    swf_header = f'; MaxNodes: {args.max_nodes}\n; MaxProcs: {args.max_procs}\n;\n'
    
    return args

if __name__ == "__main__":
    args = parse_args()
    stream_name = args.redis_stream

    # TODO: update hostname for cloudlab
    r = redis.Redis(host='head.testbed.schedulingpower.emulab.net', port=6379, decode_responses=True)
    last_id = '$'

    # List to the stream
    while True:

        # Wait for new entry
        latest = r.xread({stream_name: last_id}, None, 0)[0]

        # Parse stream entry
        stream_name = latest[0]
        stream_data = latest[1]
        for data in stream_data:
            redis_id = data[0]
            job_id = data[1]['job_id']
            event_type = data[1]['event_type']
            event_code = data[1]['event_code']
            process_stream_entry(data)
            last_id = redis_id

    # TODO: When the program terminates dump the info
    # collected as a SWF file.
