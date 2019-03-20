import argparse
import sys

parser = argparse.ArgumentParser(description="Block zones given via stdin or file for given campaign")
parser.add_argument('input_filename', nargs='?')

args = parser.parse_args()
zones = []
if args.input_filename:
    with open(args.input_filename) as f:
        zones = f.read()    
elif not sys.stdin.isatty():
    print 'reading stdin...'
    zones = sys.stdin.read()
else:
    parser.print_help()
print 'zones, ', zones

