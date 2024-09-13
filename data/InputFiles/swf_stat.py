"""
The script reads a SWF file and generates the following files:
- Job database file
- Submit event trace
- Run event trace
- End event trace
"""

def read_swf(filename):
  """
  Reads the input from the specified file and processes each line into a list of integers.

  Args:
    filename: The name of the file to read.

  Returns:
    A list of lists, where each inner list represents a processed line of input.
  """
  data = []

  with open(filename, 'r') as file:
    for line in file:
      
      # TODO: For now ignoring the header of the swf file
      if line[0] == ';':
        continue

      # Split the line into elements, convert non-empty elements to integers
      row = [int(x) for x in line.split() if x]
      data.append(row)

  return data

filename = 'interpid2009.swf'

processed_data = read_swf(filename)

for row in processed_data:
  if row[4] % 4 == 0:
    continue
  else:
    print(row)
    break