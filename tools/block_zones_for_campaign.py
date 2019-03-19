import argparse
import sys

parser = argparse.ArgumentParser(description="Block zones given via stdin or file for given campaign")
parser.add_argument('input_file', nargs='?', type=argparse.FileType('r'))

args = parser.parse_args()
zones = []
if args.input_file:
    print 'reading file'
    zones = args.input_file.read()    
elif not sys.stdin.isatty():
    print 'reading stdin...'
    zones = sys.stdin.read()
else:
    parser.print_help()
print 'zones, ', zones

