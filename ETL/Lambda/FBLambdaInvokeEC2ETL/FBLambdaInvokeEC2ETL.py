# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 14:00:54 2017

@author: Wojtek
"""

import boto3
import paramiko

bucket = 'deepdockeys'
keyfilename = 'ETLFacebook.pem'

instance_id = 'i-071dae9631dee08d3'
user = 'ubuntu'

tmppath = '/tmp/ETLFacebook.pem'
#wintmppath = 'C:\\tmp\\ETLFacebook.pem'

def startInstance(instance_id):
    ec2 = boto3.resource('ec2')
    ec2.instances.filter(InstanceIds=[instance_id]).start()
    
def getInstanceDNS(instance_id):
    while True:
        try:
            client = boto3.client('ec2')
            response = client.describe_instances(InstanceIds = [instance_id])
            foo = response['Reservations'][0]['Instances'][0]['NetworkInterfaces'][0]['Association']['PublicDnsName']
            return foo
        except:
            continue

def lambda_handler(event, context):

    # Start EC2 instance
    startInstance(instance_id)
    # Get instance IPv4
    host = getInstanceDNS(instance_id)
    # Download private key file from secure S3 bucket
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket,keyfilename,  tmppath)
    # Podaj private key z pliku
    k = paramiko.RSAKey.from_private_key_file(tmppath)
    # Stwórz połączenie SSH z serverem
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print("Connecting to " + host)
    c.connect( hostname = host, username = user, pkey = k )
    print("Connected to " + host)
    
    # Wykonaj polecenia na EC2
    commands = [
        "python /home/ubuntu/scripts/DD/EC2ETL.py",
        ]
    for command in commands:
        print("Executing {}".format(command))
        try:
            stdin , stdout, stderr = c.exec_command(command, timeout=20)
        except:
            pass
        
    c.close()
    return