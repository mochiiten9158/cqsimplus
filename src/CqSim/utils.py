import shutil

swf_columns = [
    'id',           #1 
    'submit',       #2
    'wait',         #3
    'run',          #4
    'used_proc',    #5
    'used_ave_cpu', #6
    'used_mem',     #7
    'req_proc',     #8
    'req_time',     #9
    'req_mem',      #10
    'status',       #11
    'user_id',      #12
    'group_id',     #13
    'num_exe',      #14
    'num_queue',    #15
    'num_part',     #16
    'num_pre',      #17
    'think_time',   #18
    ]

def copy_file(source, destination):
  """
  Copies a file from one location to another.

  Args:
    source: The path to the source file.
    destination: The path to the destination file.
  """
  try:
    shutil.copy2(source, destination)
    print(f"File copied from {source} to {destination}")
  except FileNotFoundError:
    print(f"Source file not found: {source}")
  except PermissionError:
    print(f"Permission denied to write to {destination}")
  except Exception as e:
    print(f"An error occurred: {e}")