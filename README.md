# logs-manager

Hi,

This project objective is to send log file to S3 and parse thoses files with EC2 parser instance.
S3 bucket have notification enable and send an event to an SQS queue. Parser instance poll this queue and retrieve file directly from a specified S3 bucket.

log-sender.py script watch a directory and send new file in a S3 bucket.


Usage log-sender /path/to/my/directory