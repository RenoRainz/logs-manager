# logs-manager

Hi,

This project objective is to send log file to S3 and parse thoses files with EC2 parser instance.
S3 bucket have notification enable and send an event to an SQS queue. Parser instance poll this queue and retrieve file directly from a specified S3 bucket.
This version works with an API web server.

log-sender.py : send file to S3 bucket.
log-sender.yml : configuration for log-sender.py
Syntaxe : log-sender.py [--config /path/to/config_file] [--dump True]

log-consumer.py : retrieve file from a S3 bucket.
log-consumer.yml : configuration for log-consumer.py
Syntaxe : log-consumer.py [--config /path/to/config_file] [--dump True]
