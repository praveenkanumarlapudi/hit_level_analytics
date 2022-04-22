import json
import os
import logging
import boto3
import urllib.parse



logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Defining boto3 entry points
gl = boto3.client('glue')
s3 = boto3.client('s3')

# Variables for the job: 
glueJobName = "HIT_LEVEL_ANALYSIS"

# Define Lambda function
def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))
    # Reading Key of incoming file
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info('input for Glue job :'+key)
    
    logger.info('## Initiating Gluejob: ')
    logger.info('## input file to process : '+"s3://"+bucket+"/"+key)

    response = gl.start_job_run(
                JobName = "HIT_LEVEL_ANALYSIS",
                Arguments = {
                    '--input_file_to_process': 's3://'+bucket+"/"+key,
                    } )
    
    logger.info('## STARTED GLUE JOB: ' + glueJobName)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response