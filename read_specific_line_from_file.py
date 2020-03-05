import linecache
import os
import sys

file_path = sys.argv[1]
line_number = int(sys.argv[2])

# Print file size
file_stats = os.stat(file_path)
print('File size: ' + str(file_stats.st_size) + ' bytes')

# Print specific lines in file
with open(file_path, 'r') as file:
    for i, line in enumerate(file):
        if i == line_number:
            print(line)
            break

# Print total number of lines in file
with open(file_path, 'r') as file:
    for i, line in enumerate(file):
        pass
    
    print(str(i + 1) + ' lines in file, \'' + file_path + '\'')
