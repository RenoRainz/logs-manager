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
import json
import re


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


	# Browse pid dir to know shm name for log_sender
	pids_list = list()
	for dirname, dirnames, filenames in os.walk(pid_dir_log_sender):
		# print path to all filenames.
		for filename in filenames:
			# add pid to the list
			pids_list.append(int(filename))

	# Loop in pids list and open shm for each pid
	shm_list = list()
	shm_mapfile_list = list()
	for pid in pids_list:
		# Try reading SHM
		# Now we have a pid store in a file, we can instanciate
		# a SHM fd to communicate with the WEB API with the pid number
		shm_name = "/" + "log_sender_" + str(pid)
		shm_list.append(shm_name)

		try:
			shm = posix_ipc.SharedMemory(shm_name, size = 1024, mode = 400)
		except:
			error = sys.exc_info()[0]
			print "Error creating SHM. Error type %s" %  error
			sys.exit(4)

		# MMap the shared memory
		try:
			shm_mapfile = mmap.mmap(shm.fd, shm.size)
			shm_mapfile_list.append(shm_mapfile)
			shm.close_fd()
		except:
			error = sys.exc_info()[0]
			print "Error mapping SHM. Error type %s" %  error
			sys.exit(4)

	# loop in shm to read them
	instance_num = 0
	for shm_mapfile in shm_mapfile_list:

		output = ""
		shm_mapfile.seek(0)
		shm_mapfile_size = shm_mapfile.size()
		output = shm_mapfile.read(int(shm_mapfile_size))
		instance_num += 1
		print "Instance #%s : %s \n" % (str(instance_num), json.dumps(json.loads(del_illegal_char(output)), separators=(',', ':'), sort_keys=True, indent=4 ))

def del_illegal_char(mystring):

	"""
	Function to remove unwanted character.
	Thx to Chase Seibert.
	http://chase-seibert.github.io/blog/2011/05/20/stripping-control-characters-in-python.html
	"""

	RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + \
					u'|' + \
					u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
					(unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),
					unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),
					unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),
					)

	mystring = re.sub(RE_XML_ILLEGAL, "", mystring)
	return mystring



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
