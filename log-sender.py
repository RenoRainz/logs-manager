#!/usr/bin/python
##########################################################
#
# Send log in a S3 bucket
#
# Version: 2015-09-23
#
##########################################################

#import boto
import sys
import os
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import argparse
import shutil
import gzip


# Global variables
INPUT_QUEUE_NAME  = 'syslog-input'
BUCKET_NAME = 'syslog-input'
REGION = 'eu-west-1'
CREDENTIALS_FILE = 'credentials/credentials.txt'


##########################################################
# Connect to S3 and send logs
##########################################################
def main(argv=None):

	# Get api acces keys
	myfile = open(CREDENTIALS_FILE, 'r')
	for line in myfile:
		if line.split()[0] == 'log-manager':
			acces_key = line.split()[1]
			secret_key = line.split()[2]
	myfile.close()

	# Get args
	args = get_args()

	# Directory to watch
	walk_dir = args.directory

	# Compression
	compress = args.compress
	compress_dir = args.compress_dir

	# TODO : need to put this in a infinite loop

	# Browse into the directory and check when
	# the file was modified, if > 1 we process it
	for root, subdirs, files in os.walk(walk_dir):
		for name in files:
			full_path = os.path.join(root, name)
			relative_path = os.path.relpath(full_path, walk_dir)
			last_modification = os.path.getmtime(full_path)
			now = time.time()
			delta = (int(now) - int(last_modification))
			if delta > 60 :
				#print 'send %s to bucket %s/%s' % (full_path, BUCKET_NAME, relative_path)
				sent_to_s3(acces_key, secret_key, BUCKET_NAME, relative_path, full_path, compress, compress_dir)

def get_args():
	"""
    Use the tools.cli methods and then add a few more arguments.
    """
	parser = argparse.ArgumentParser(description='Sent log file to S3')
	parser.add_argument('--directory', type=str, help='Directory to watch', required=True)
	parser.add_argument('--compress', type=bool, required=False, help='Activate compression', default=False)
	parser.add_argument('--compress_dir', type=str, help='Directory to use during compression', required=False, default='/tmp')

	args = parser.parse_args()

	return args


def sent_to_s3(acces_key, secret_key, bucket_name, key, filename, compress, compress_dir):

	"""
	Function to sent a file to S3 bucket
	"""

	# TODO: put this block in try
	s3_conn = S3Connection(acces_key, secret_key)
	bucket = s3_conn.get_bucket(bucket_name)
	bucket_key = Key(bucket)

	# Todo : Fix this
	if compress:
		key = str(key) + ".gz"
		print key

	bucket_key.key = str(key)

	if compress:
		print "Compression before sending"
		# copy file to compress dir
		compress_file = compress_dir + "/" + str(os.path.basename(filename)) + ".gz"
		shutil.copyfile(filename, compress_file)

		# Compression
		with open(filename, 'rb') as f_in, gzip.open(compress_file, 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)

		sent_file = open(compress_file, 'r')
		#print "Sent to S3 desactivated"
		bucket_key.set_contents_from_file(sent_file, replace=True, rewind=True)
		sent_file.close()

		# Delete file
		delete_file(compress_file)

	else:
		sent_file = open(filename, 'r')
		#print "Sent to S3 desactivated"
		bucket_key.set_contents_from_file(sent_file, replace=True, rewind=True)
		sent_file.close()

		# Delete file
		delete_file(filename)

def delete_file(filename):

	"""
	Function to delete a proceed file
	"""

	if os.path.exists(filename):
		print "delete file %s" % filename
		print "Delete desactivated"
		#os.remove(filename)

if __name__ == "__main__":
	sys.exit(main())
