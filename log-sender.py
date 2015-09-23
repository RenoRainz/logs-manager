#!/usr/bin/python
##########################################################
#
# Send log in a S3 bucket
#
# Version: 2015-09-23
#
##########################################################

import boto
import json
import time
import sys
import os
import logging
import StringIO
import uuid
import math
import httplib
from boto.sqs.message import RawMessage
from boto.sqs.message import Message
from boto.s3.key import Key

# Global variables
INPUT_QUEUE_NAME  = 'syslog-input'
BUCKET_NAME = 'syslog-input'
REGION = 'eu-west-1'

new_message = old_message = ''

##########################################################
# Connect to S3 and send logs
##########################################################
def main(argv=None):

	global new_message, old_message

	
	# Set S3 endpoint based on region (eg 's3-eu-west-1.amazonaws.com')
	s3_endpoint = [region.endpoint for region in boto.s3.regions() if region.name == region_name][0]

	# Wait until everything is ready
	while True:

		# S3 OK?
		s3_ok, s3_connection, s3_output_bucket = setup_s3(BUCKET_PREFIX, s3_endpoint, region_name)

		# SQS OK?
		sqs_ok, input_queue, output_queue = setup_sqs(INPUT_QUEUE_NAME, OUTPUT_QUEUE_NAME, region_name)

		# All good!
		if s3_ok and sqs_ok:
			break;

		# Log error message if it has changed
		if new_message != old_message:
			error_message(new_message)

			# If SQS is alive, send it there, too
			if sqs_ok:
				write_output_message(new_message, output_queue)

			old_message = new_message
			new_message = ''

		time.sleep(20)

	info_message('Setup complete.\nInput queue: %s\nOutput queue: %s\nS3 Bucket: %s in region %s' % (input_queue.name, output_queue.name, s3_output_bucket.name, s3_output_bucket.get_location()))

	# Main loop
	while True:
		process_queue(input_queue, output_queue, s3_output_bucket)
		time.sleep(5)


##############################################################################
# Process a newline-delimited list of URls
##############################################################################
def process_queue(input_queue, output_queue, s3_output_bucket):

	# Get messages
	info_message("Polling input queue...")
	rs = input_queue.get_messages(num_messages=1, wait_time_seconds=20)

	if len(rs) > 0:
		# Iterate each message
		for raw_message in rs:
			info_message("Message received...")

			# Parse JSON message (going two levels deep to get the embedded message)
			message = raw_message.get_body()

			# Create a unique job id
			job_id = str(uuid.uuid4())

			# Sleep for a while to simulate a heavy workload
			# (Otherwise the queue empties too fast!)
			info_message('Processing...')
			time.sleep(15)
	
			# Process the image, creating the image montage
			output_url = process_message(message, s3_output_bucket, job_id)
		
			if output_url is None:
				output_message = "An error occurred. Please ask your instructor to review the log file."
			else:
				output_message = output_url
	
			# Write message to output queue
			write_output_message(output_message, output_queue)
		
			info_message("Image processing completed.")
		
			# Delete message from the queue
			input_queue.delete_message(raw_message)


##############################################################################
# Process a newline-delimited list of URls
##############################################################################
def process_message(message, s3_output_bucket, job_id):
	try:
		output_dir = "/home/ec2-user/jobs/%s/" % (job_id)
	
		# Download images from URLs specified in message
		for line in message.splitlines():
			info_message("Downloading image from %s" % line)
			os.system("wget -P %s %s" % (output_dir, line))

		output_image_name = "output-%s.jpg" % (job_id)
		output_image_path = output_dir + output_image_name 
	
		# Invoke ImageMagick to create a montage
		info_message("Creating Montage")
		os.system("montage -size 400x400 null: %s*.* null: -thumbnail 400x400 -bordercolor white -background black +polaroid -resize 80%% -gravity center -background black -geometry -10+2  -tile x1 %s" % (output_dir, output_image_path))
	
		# Write the resulting image to s3
		output_url = write_image_to_s3(output_image_path, output_image_name, s3_output_bucket)
	
		# Return the output url
		return output_url
	except:
		error_message("An error occurred. Please show this to your class instructor.")
		error_message(sys.exc_info()[0])
		
##############################################################################
# Write the result of a job to the output queue
##############################################################################		
def write_output_message(message, output_queue):
	info_message("Message sent to Queue: %s" % message)
	m = RawMessage()
	m.set_body(message)
	status = output_queue.write(m)
	
##############################################################################
# Write an image to S3
##############################################################################
def write_image_to_s3(path, file_name, s3_output_bucket):
	
	# Create a key to store the instances_json text
	info_message('Starting to write to S3.')
	k = Key(s3_output_bucket)
	k.key = "out/" + file_name
	k.set_metadata("Content-Type", "image/jpeg")
	k.set_contents_from_filename(path)
	k.set_acl('public-read')
	info_message('Finished writing to S3.')
	
	# Return a URL to the object
	url = "https://%s.s3.amazonaws.com/%s" % (s3_output_bucket.name, k.key)
	info_message("Image uploaded to S3: %s" % url)
	return url
	
##############################################################################
# Verify S3 bucket
# Return: s3_ok, s3_connection, s3_output_bucket
##############################################################################
def setup_s3(s3_bucket_prefix, s3_endpoint, region_name):

	global new_message

	try:
		s3_connection = boto.connect_s3(host=s3_endpoint)
			
		# Use the first bucket starting with 'image-bucket'
		buckets = [bucket for bucket in s3_connection.get_all_buckets() if bucket.name.lower().startswith(s3_bucket_prefix)]
		if len(buckets) > 0:
			bucket = buckets[0]
			bucket_location = bucket.get_location()
			if bucket_location == '':
				s3_connection = boto.connect_s3()
			else:
				s3_endpoint = [region.endpoint for region in boto.s3.regions() if region.name == bucket_location][0]
				s3_connection = boto.connect_s3(host=s3_endpoint)  # Reconnect in case it's in a different region
			s3_bucket = s3_connection.get_bucket(bucket.name)
			return True, s3_connection, s3_bucket
		
		# No bucket found
		new_message += "Unable to access S3 bucket. Please check that a bucket has been created with a name starting with 'image-bucket'. "
		return False, None, None

	except Exception as ex:
		new_message += "Unable to connect to S3. Check IAM Permissions on EC2 role. "
		return False, None, None
		
##############################################################################
# Verify SQS permissions, create if required
# Return: sqs_ok, input_queue, output_queue
##############################################################################
def setup_sqs(input_queue_name, output_queue_name, region_name):

	global new_message
	input_ok  = False
	output_ok = False

	try:
		# Connect to SQS
		sqs_connection = boto.sqs.connect_to_region(region_name)

		# Input Queue
		all_queues = [q.name for q in sqs_connection.get_all_queues() if q.name.lower() == input_queue_name]
		if len(all_queues) > 0:
			input_queue = sqs_connection.get_queue(all_queues[0])
			input_queue.set_message_class(RawMessage)
			input_ok = True
		else:
			# No queue found
			new_message += "SQS '" + input_queue_name + "' queue not found. "

		# Output Queue
		all_queues = [q.name for q in sqs_connection.get_all_queues() if q.name.lower() == output_queue_name]
		if len(all_queues) > 0:
			output_queue = sqs_connection.get_queue(all_queues[0])
			output_ok = True
		else:
			# No queue found, so create one!
			new_message += "SQS '" + output_queue_name + "' queue not found. "

		if input_ok and output_ok:
			return True, input_queue, output_queue
		else:
			return False, None, None

	except Exception as ex:
		new_message += "Unable to connect to SQS. Check IAM Permissions on EC2 role. "
		return False, None, None

##############################################################################
# Use logging class to log simple info messages
##############################################################################
def info_message(message):
	message = time.strftime("%Y-%m-%d %H:%M:%S ") + str(message)
	print message
	logger.info(message)

def error_message(message):
	message = time.strftime("%Y-%m-%d %H:%M:%S ") + str(message)
	print message
	logger.error(message)

##############################################################################
# Generic string logging
##############################################################################
class Logger:
	def __init__(self):
		#self.stream = StringIO.StringIO()
		#self.stream_handler = logging.StreamHandler(self.stream)
		self.file_handler = logging.FileHandler('/home/ec2-user/image_processor.log')
		self.log = logging.getLogger('image-processor')
		self.log.setLevel(logging.INFO)
		for handler in self.log.handlers: 
			self.log.removeHandler(handler)
		self.log.addHandler(self.file_handler)
		
	def info(self, message):
		self.log.info(message)
		
	def error(self, message):
		self.log.error(message)

logger = Logger()

if __name__ == "__main__":
	sys.exit(main())
