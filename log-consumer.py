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
#from boto.sqs import SQS
import boto
from boto.sqs.message import RawMessage
import argparse
import shutil
import gzip

# Global variables
INPUT_QUEUE_NAME  = 'syslog-input'
BUCKET_NAME = 'syslog-input'
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


    # TODO: put this block in try
	sqs_conn = boto.sqs.connect_to_region(REGION, aws_access_key_id=acces_key, aws_secret_access_key=secret_key)
	print sqs_conn

    # Get queue
	queue = sqs_conn.get_queue('syslog-input')

	# Retrieve messages
	retrieve_msg(acces_key, secret_key, sqs_conn, queue)


def retrieve_msg(acces_key, secret_key, sqs_conn, queue):

	"""
	Function to retrieve a msg from a SQS queue
	"""

	msg = sqs_conn.receive_message(queue, 5)

	print "MSG : "
	print type(msg)
	print msg



if __name__ == "__main__":
	sys.exit(main())
