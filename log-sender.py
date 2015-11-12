#!/usr/bin/python
##########################################################
#
# Send log in a S3 bucket
#
# Version: 2015-09-23
#
##########################################################

import boto
import sys
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import argparse
import shutil
import gzip
import yaml
import posix_ipc
import mmap
import time
import json

##########################################################
# Connect to S3 and send logs
##########################################################
def main(argv=None):

	# Get args
	args = get_args()

	# Get configuration file
	if args.config:
		conf_file = args.config
	else:
		conf_file = 'log-sender.yml'

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
	if 'delay_time' in conf_map['global']:
		delay_time = conf_map['global']['delay_time']
	else:
		# Set a default value
		delay_time = '60'

	if 'bucket_name' in conf_map['global']:
		bucket_name = conf_map['global']['bucket_name']
	else:
		print "bucket_name value not found in %s. Exiting ..." % conf_file
		sys.exit(2)

	if 'credentials_file' in conf_map['global']:
		credentials_file = conf_map['global']['credentials_file']
	else:
		print "credentials_file value not found in %s. Exiting ..." % conf_file
		sys.exit(2)

	if 'region' in conf_map['global']:
		region = conf_map['global']['region']
	else:
		print "region value not found in %s. Exiting ..." % conf_file
		sys.exit(2)

	if 'input_queue_name' in conf_map['global']:
		input_queue_name = conf_map['global']['input_queue_name']
	else:
		print "input_queue_name value not found in %s. Exiting ..." % conf_file
		sys.exit(2)

	if 'watch_directory' in conf_map['global']:
		watch_directory = conf_map['global']['watch_directory']
	else:
		print "watch_directory value not found in %s. Exiting ..." % conf_file
		sys.exit(2)

	if 'compress' in conf_map['global']:
		compress = conf_map['global']['compress']
	else:
		# Set default value to True
		compress = True

	if 'compress_dir' in conf_map['global']:
		compress_dir = conf_map['global']['compress_dir']
	else:
		# Set default value to /tmp
		compress_dir = '/tmp'

	if 'pid_dir' in conf_map['global']:
		pid_dir = conf_map['global']['pid_dir']
	else:
		# Set default value to /tmp
		pid_dir = '/var/run/log-sender'

	# Clean pid_dir of old and used pid dir
	clean_pid(pid_dir)


	# We create a  pid file for each instance
	pid = os.getpid()
	if not os.path.exists(pid_dir):
		# We create the directory
		try:
			os.makedirs(pid_dir)
		except :
			error = sys.exc_info()[0]
			print "Error creating dir %s. Error type %s" % (pid_dir, error)
			sys.exit(4)

	pid_file = str(pid_dir) + "/" + str(pid)
	try:
		pid_fd = open(pid_file,'w')
		pid_fd.write(str(pid))
		pid_fd.close()
	except:
		error = sys.exc_info()[0]
		print "Error creating file %s. Error type %s" % (pid_file, error)
		sys.exit(4)


	# Now we have a pid store in a file, we can instanciate
	# a SHM fd to communicate with the WEB API with the pid number
	shm_name = "/" + "log_sender_" + str(pid)

	try:
		shm = posix_ipc.SharedMemory(shm_name, posix_ipc.O_CREX, size = 1024, mode = 0600)
	except:
		error = sys.exc_info()[0]
		print "Error creating SHM. Error type %s" %  error
		sys.exit(4)

	# MMap the shared memory
	try:
		shm_mapfile = mmap.mmap(shm.fd, shm.size)
		shm.close_fd()
	except:
		error = sys.exc_info()[0]
		print "Error mapping SHM. Error type %s" %  error
		sys.exit(4)

	# Get api acces keys
	myfile = open(credentials_file, 'r')
	for line in myfile:
		if line.split()[0] == 'log-manager':
			acces_key = line.split()[1]
			secret_key = line.split()[2]
	myfile.close()

	s3_conn = S3Connection(acces_key, secret_key)
	try:
		bucket = s3_conn.get_bucket(bucket_name)
	except :
		error = sys.exc_info()[0]
		print "S3 error : %s" % error
		sys.exit(3)

	# TODO : need to put this in a infinite loop
	files_proceed_counter = 0
	files_failed_counter = 0
	stats = dict()
	stats['nb_file_proceed'] = 0
	stats['nb_file_failed'] = 0
	files_proceed = list()
	files_failed = list()

	# Browse into the directory and check when
	# the file was modified, if > 1 we process it
	for root, subdirs, files in os.walk(watch_directory):
		for name in files:
			full_path = os.path.join(root, name)
			relative_path = os.path.relpath(full_path, watch_directory)
			last_modification = os.path.getmtime(full_path)
			now = time.time()
			delta = (int(now) - int(last_modification))
			if delta > int(delay_time) :
				rc_sent_to_s3 = sent_to_s3(s3_conn, bucket, relative_path, full_path, compress, compress_dir)

				# Update stats if ok
				if rc_sent_to_s3:
					files_proceed_counter += 1
					stats['nb_file_proceed'] = files_proceed_counter
					files_proceed.append(str(full_path))
					stats['files_proceed'] = files_proceed


				else:
					files_failed_counter += 1
					stats['nb_file_failed'] = files_failed_counter
					files_failed.append(str(full_path))
					stats['files_failed'] = files_failed

				# Convert dict to json
				stats_json = json.dumps(stats)
				print stats_json
				# Update SHM
				shm_mapfile.seek(0)
				shm_mapfile.write(stats_json)

	time.sleep(60)

	# At this end, remove pid_dir
	os.remove(pid_file)

	# Closing SHM
	shm_mapfile.close()

	# Delete SHM
	shm.unlink()


def get_args():
	"""
    Use the tools.cli methods and then add a few more arguments.
    """
	parser = argparse.ArgumentParser(description='Sent log file to S3')
	parser.add_argument('--config', type=str, help='Path to the configuration file', required=False)
	parser.add_argument('--dump', type=bool, help='Dump configuration file', required=False)

	args = parser.parse_args()

	return args

def clean_pid(pid_dir):

	"""
	Function to delete unused pid dir.
	"""

	# Browse pid dir to know shm name
	for dirname, dirnames, filenames in os.walk(pid_dir):
		# print path to all filenames.
		for filename in filenames:
			print "file : %s" % os.path.join(dirname, filename)

			# Check if process still exist
			try :
				os.kill(int(filename), 0)
			except OSError:
				# Delete file
				os.remove(os.path.join(dirname, filename))

def sent_to_s3(s3_conn, bucket, key, filename, compress, compress_dir):

	"""
	Function to sent a file to S3 bucket
	"""

	# Return code
	rc = True

	# Todo : Fix this
	if compress:
		key = str(key) + ".gz"

	bucket_key = Key(bucket)
	if isinstance(bucket_key, boto.s3.key.Key):
		bucket_key.key = str(key)
	else:
		print "Error accessing bucket key : %s" % bucket_key
		sys.exit(3)

	if compress:
		# copy file to compress dir
		compress_file = compress_dir + "/" + str(os.path.basename(filename)) + ".gz"

		# Compression
		try:
			with open(filename, 'rb') as f_in, gzip.open(compress_file, 'wb') as f_out:
				shutil.copyfileobj(f_in, f_out)
		except:
			print "Error compressing %s " % compress_file
			#sys.exit(3)
			rc = False

		try:
			sent_file = open(compress_file, 'r')
			print "Sent to S3 desactivated"
			#bucket_key.set_contents_from_file(sent_file, replace=True, rewind=True)
			sent_file.close()
			#print "File %s sent." % filename
		except:
			print "Error sending %s to bucket key : %s" % (compress_file, bucket_key)
			#sys.exit(3)
			rc = False

		# Delete file
		delete_file(compress_file)

	else:
		try:
			sent_file = open(filename, 'r')
			#print "Sent to S3 desactivated"
			bucket_key.set_contents_from_file(sent_file, replace=True, rewind=True)
			sent_file.close()
			#print "File %s sent." % filename
		except:
			print "Error sending %s to bucket key : %s" % (compress_file, bucket_key)
			#sys.exit(3)
			rc = False



		# Delete file
		# TODO : handle return of this function.
		delete_file(filename)

	return rc

def delete_file(filename):

	"""
	Function to delete a proceed file
	"""

	if os.path.exists(filename):
		print "Delete desactivated"
		#os.remove(filename)

if __name__ == "__main__":
	sys.exit(main())
