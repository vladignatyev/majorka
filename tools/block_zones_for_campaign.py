import argparse
import sys

parser = argparse.ArgumentParser(description="huy")
parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'))

args = parser.parse_args()
zones = []
if args.input_file:
    print 'reading file'
    zones = args.input_file.read()    
elif not sys.stdin.isatty():
    print 'reading stdin...'
    print sys.stdin.read()
else:
    parser.print_help()

