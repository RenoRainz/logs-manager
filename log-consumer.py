#!/usr/bin/python
##########################################################
#
# Pull a SQS object to get notified
# and retrieve log file in a S3 bucket
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
import yaml


##########################################################
# Connect to SQS and retrieve message
##########################################################
def main(argv=None):

	# Get args
	args = get_args()

	# Get configuration file
	if args.config:
		conf_file = args.config
	else:
		conf_file = 'log-consumer.yml'

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
	if 'interval_time' in conf_map['global']:
		interval_time = conf_map['global']['interval_time']
	else:
		# Set a default value
		interval_time = '10'

	if 'store_directory' in conf_map['global']:
		store_directory = conf_map['global']['store_directory']
	else:
		print "store_directory value not found in %s. Exiting ..." % conf_file
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

	# Get api acces keys
	myfile = open(credentials_file, 'r')
	for line in myfile:
		if line.split()[0] == 'log-manager':
			acces_key = line.split()[1]
			secret_key = line.split()[2]
	myfile.close()


    # TODO: put this block in try
	# Connection to SQS
	sqs_conn = boto.sqs.connect_to_region(region, aws_access_key_id=acces_key, aws_secret_access_key=secret_key)
	# Connection to S3
	s3_conn = S3Connection(acces_key, secret_key)

    # Get queue
	queue = sqs_conn.get_queue(input_queue_name)

	# TODO : put all of this in a while

    # Retrieve messages
	msg = retrieve_msg(sqs_conn, queue)

	if msg :
		record = msg.get_body()
		record_json = json.loads(record)

		# Extracting informations : bucket name and key
		if 'Records' in record_json:
			if 's3' in record_json['Records'][0]:
				bucket = record_json['Records'][0]['s3']['bucket']['name']
				s3_key =  record_json['Records'][0]['s3']['object']['key']

				# Retrieve file from S3
				file_to_parse = retrieve_file(s3_conn, bucket, s3_key, store_directory)
				print "file %s retrieved" % file_to_parse

				# if file exist, we delete msg in SQS
				if os.path.isfile(file_to_parse):
					# We can delete the msg
					delete_msg(sqs_conn, queue, msg)



def get_args():

	"""
    Use the tools.cli methods and then add a few more arguments.
    """

	parser = argparse.ArgumentParser(description='Check SQS for notification and Get log file from S3')
	parser.add_argument('--config', type=str, help='Path to the configuration file', required=False)
	parser.add_argument('--dump', type=bool, help='Dump configuration file', required=False)

	args = parser.parse_args()

	return args

def retrieve_file(s3_conn, bucket_name, key_name, output_dir):

	"""
	Function to retrieve a msg from a SQS queue
	"""

	# get_contents_to_file
	bucket = s3_conn.get_bucket(bucket_name)
	key = bucket.get_key(key_name)

	if key :
		output_file = output_dir + key_name
		output_dir = os.path.dirname(output_file)

		# Check if directory exist if not create it, then retrieve the files
		if os.path.exists(output_dir):
			key.get_contents_to_filename(output_file)
		else:
			# We create the directory
			os.makedirs(output_dir)
			key.get_contents_to_filename(output_file)

		# If compress, uncompress
		if output_file.endswith('.gz'):
			output_filename, output_file_extension = os.path.splitext(output_file)
			with open(output_filename, 'wb') as f_out, gzip.open(output_file, 'rb', 0) as f_in:
				shutil.copyfileobj(f_in, f_out)
			# deleting tmp file
			os.remove(output_file)

		return output_filename



def retrieve_msg(sqs_conn, queue):

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

def delete_msg(sqs_conn, queue, message):

	"""
	Function to delete a msg from a SQS queue
	"""

	is_deleted = sqs_conn.delete_message(queue,message)
	if is_deleted:
		print "Message deleted."
		return True

if __name__ == "__main__":
	sys.exit(main())
