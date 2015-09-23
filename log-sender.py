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
from datetime import datetime

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
	directory = str(sys.argv[1])

	sent_to_s3(acces_key, secret_key, BUCKET_NAME, '/var/log/lastlog')

def sent_to_s3(acces_key, secret_key, bucket_name, filename):

	now = datetime.now()

	s3_conn = S3Connection(acces_key, secret_key)
	bucket = s3_conn.get_bucket(bucket_name)
	bucket_key = Key(bucket)
	bucket_key.key = str(now.year) + "/" + str(now.month) + "/" + str(now.day) + "/" + str(now.hour) + "/" + os.path.basename(filename)
	sent_file = open(filename, 'r')
	bucket_key.set_contents_from_file(sent_file, replace=True, rewind=True)	
	sent_file.close()

if __name__ == "__main__":
	sys.exit(main())