#!/usr/bin/python
##########################################################
#
# Pull a SQS object to get notified
# and retrieve log in a S3 bucket
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
from boto.sqs.connection import SQSConnection
import boto
import argparse
import shutil
import gzip
import json

# Global variables
INPUT_QUEUE_NAME  = 'syslog-input'
REGION = 'eu-west-1'
CREDENTIALS_FILE = 'credentials/credentials.txt'

##########################################################
# Connect to SQS and retrieve message
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

    # TODO: put this block in try
	# Connection to SQS
	sqs_conn = boto.sqs.connect_to_region(REGION, aws_access_key_id=acces_key, aws_secret_access_key=secret_key)
	# Connection to S3
	s3_conn = S3Connection(acces_key, secret_key)

    # Get queue
	queue = sqs_conn.get_queue('syslog-input')

    # Retrieve messages
	msg = retrieve_msg(acces_key, secret_key, sqs_conn, queue)

	# Extracting informations : bucket name and key
	if msg :
		record = msg.get_body()
		record_json = json.loads(record)

		if 'Records' in record_json:
			if 's3' in record_json['Records'][0]:
				bucket = record_json['Records'][0]['s3']['bucket']['name']
				s3_key =  record_json['Records'][0]['s3']['object']['key']
				print "bucket : %s , key : %s" % (bucket, s3_key)
				# Retrieve file from S3
				retrieve_file(s3_conn, bucket, s3_key, args.directory)

def get_args():

	"""
    Use the tools.cli methods and then add a few more arguments.
    """

	parser = argparse.ArgumentParser(description='Check SQS for notification and Get log file from S3')
	parser.add_argument('--directory', type=str, help='Directory to store the file', required=True)

	args = parser.parse_args()

	return args

def retrieve_file(s3_conn, bucket_name, key_name, output_dir):

	"""
	Function to retrieve a msg from a SQS queue
	"""

	# get_contents_to_file
	bucket = s3_conn.get_bucket(bucket_name)
	key = bucket.get_key(key_name)
	print "key %s" % key
	if key :
		output_file = output_dir + key_name
		output_dir = os.path.dirname(output_file)
		print "output directory %s" % output_dir
		# Check if directory exist if not create it
		if os.path.exists(output_dir):
			key.get_contents_to_filename(output_file)
		else:
			# We create the directory
			os.makedirs(output_dir)
			key.get_contents_to_filename(output_file)

		return output_file



def retrieve_msg(acces_key, secret_key, sqs_conn, queue):

	"""
	Function to retrieve a msg from a SQS queue
	"""

	msg = queue.read()
	# Check if we get a message
	if msg and len(msg) > 0:
		return msg
	else:
		print "Queue empty"
        return None



if __name__ == "__main__":
	sys.exit(main())
