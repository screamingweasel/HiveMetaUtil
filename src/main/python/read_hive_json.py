###################################################################################################
# Description: Foo!
# Author:      J. Barnett
# History:     07/06/2017 - Initial Release
###################################################################################################
import HiveMetaUtil as util
import json
import sys
from pprint import pprint
import argparse

def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('jsonFile')
	parser.add_argument('--debug', action='store_true', default=False)
	return parser.parse_args()

def read_lines(jsonFile):
	sys.stderr.write("reading jsonFile " + jsonFile + "\n")
	f = open(jsonFile, 'r')
	lines = f.readlines()
	f.close()
	return lines

##################################################################################################
# Main
##################################################################################################
def main():
	args=parse_arguments()

	lines = read_lines(args.jsonFile)
	
	#util.print_csv(lines)

	for line in lines:
		j = json.loads(line)
		#pprint(j)
		ti = util.get_table_info(j)
		util.print_create_table(ti)
#		pprint(ti)

##################################################################################################
# Main
##################################################################################################
if __name__ == "__main__":
    main()