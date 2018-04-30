import sys

output_fname = sys.argv[1]

with open(output_fname, 'w') as fobj:
    fobj.write("this is a test output\n")
