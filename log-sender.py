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
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key

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

	# Directory to watch
	walk_dir = sys.argv[1]

	# TODO : Need to put this in infinite loop

	# Browse into the directory and check when
	# the file was modified, if > 1 we process it 
	for root, subdirs, files in os.walk(walk_dir):
		for name in files:
			full_path = os.path.join(root, name)
			relative_path = os.path.relpath(full_path, walk_dir)
			last_modification = os.path.getmtime(full_path)
			now = time.time()
			delta = (int(now) - int(last_modification)) / 60
			if delta > 1 :
				print "send %s to bucket %s" % (full_path, BUCKET_NAME)
				sent_to_s3(acces_key, secret_key, BUCKET_NAME, relative_path, full_path,False)
				# TODO : Once file is sent we need to delete it

def sent_to_s3(acces_key, secret_key, bucket_name, key, filename, compress):

	s3_conn = S3Connection(acces_key, secret_key)
	bucket = s3_conn.get_bucket(bucket_name)
	bucket_key = Key(bucket)
	bucket_key.key = str(key)
	sent_file = open(filename, 'r')
	bucket_key.set_contents_from_file(sent_file, replace=True, rewind=True)	
	sent_file.close()

if __name__ == "__main__":
	sys.exit(main())