import boto3
import yaml
import sys
from . import config


def run():
	config.loadConfig(sys.argv[1])


if __name__=='__main__':
	run();