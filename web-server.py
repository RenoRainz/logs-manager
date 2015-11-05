#!/usr/bin/python
##########################################################
#
# Web interface to interact with log-consumer/log-sender
#
# Version: 2015-11-06
#
##########################################################

#!/usr/bin/python
##########################################################
#
# Send log in a S3 bucket
#
# Version: 2015-09-23
#
##########################################################

import sys
import os
import argparse
import yaml
import posix_ipc
import mmap
import time


def main(argv=None):

	# Get args
	args = get_args()

	# Get configuration file
	if args.config:
		conf_file = args.config
	else:
		conf_file = 'web-server.yml'

	# Parsing config file
	if os.path.isfile(conf_file):
		f = open(conf_file, 'rb')
		conf_map = yaml.safe_load(f)
		f.close()
	else:
		print "Configuration file not found. Exiting ..."
		sys.exit(1)

	# Dump configuration file and exit
	if args.dump:
		print yaml.dump(conf_map)
		sys.exit(0)

	# Setting up configuration variables
	if 'pid_dir_log_sender' in conf_map['global']:
		pid_dir_log_sender = conf_map['global']['pid_dir_log_sender']
	else:
		print "Pid dir for log_sender is missing in config file %s. Exiting ..." % pid_dir_log_sender
		sys.exit(2)

	if 'pid_dir_log_consumer' in conf_map['global']:
		pid_dir_log_consumer = conf_map['global']['pid_dir_log_consumer']
	else:
		print "Pid dir for log_consumer is missing in config file %s. Exiting ..." % pid_dir_log_consumer
		sys.exit(2)


	for dirname, dirnames, filenames in os.walk(pid_dir_log_sender):
		# print path to all subdirectories first.
		for subdirname in dirnames:
			print(os.path.join(dirname, subdirname))
		# print path to all filenames.
		for filename in filenames:
			print(os.path.join(dirname, filename))


def get_args():
	"""
    Use the tools.cli methods and then add a few more arguments.
    """
	parser = argparse.ArgumentParser(description='Web Server to ')
	parser.add_argument('--config', type=str, help='Path to the configuration file', required=False)
	parser.add_argument('--dump', type=bool, help='Dump configuration file', required=False)

	args = parser.parse_args()

	return args



if __name__ == "__main__":
	sys.exit(main())
