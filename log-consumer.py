#!/usr/bin/python
##########################################################
#
# Pull a SQS to get notified
# and get log in a S3 bucket
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
