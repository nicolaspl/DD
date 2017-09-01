# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 10:19:09 2017

@author: Wojtek
"""

import boto3
import subprocess
import datetime
import paramiko

keyfilename = 'ETLFacebook.pem'
keypath = '/home/ubuntu/keys/ETLFacebook.pem'
user = 'ubuntu'

this_instance_id = 'i-071dae9631dee08d3'
DM_instance_id = 'i-0f1ed5a14c64d8373'
logs_path = '/home/ubuntu/logs/'

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
def stopEC2Instance(instance_id):
    ec2 = boto3.resource('ec2')
    ec2.instances.filter(InstanceIds=[instance_id]).stop()
    

date = datetime.datetime.today()
today = str(date.date()) + '_' + str(date.time())


#process = subprocess.Popen('python /home/ubuntu/scripts/DD/EC2UploadFBDataFromJSONs.py > ' + logs_path + 'EC2UploadFBDataFromJSONs/' + today + '.txt', shell=True, stdout=subprocess.PIPE)
#process.wait()
#print(process.returncode)
#
#process = subprocess.Popen('python3 /home/ubuntu/scripts/DD/EC2UploadImageProcessingData.py > ' + logs_path + 'EC2UploadImageProcessingData/' + today + '.txt' , shell=True, stdout=subprocess.PIPE)
#process.wait()
#print(process.returncode)

# Start EC2 instance
startInstance(DM_instance_id)
# Get instance IPv4
host = getInstanceDNS(DM_instance_id)
# Download private key file from secure S3 bucket
#    s3_client = boto3.client('s3')
#    s3_client.download_file(bucket,keyfilename,  wintmppath)
# Podaj private key z pliku
k = paramiko.RSAKey.from_private_key_file(keypath)
# Stwórz połączenie SSH z serverem
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

print("Connecting to " + host)
c.connect( hostname = host, username = user, pkey = k )
print("Connected to " + host)

# Wykonaj polecenia na EC2
commands = [
    "python /home/ubuntu/scripts/DD/EC2DM.py"
    ]
for command in commands:
    print("Executing {}".format(command))
    try:
        stdin , stdout, stderr = c.exec_command(command, timeout=10)
    except:
        pass
    
c.close()

print('Stopping EC2 instance')
stopEC2Instance(this_instance_id)
