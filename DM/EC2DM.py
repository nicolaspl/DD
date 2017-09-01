# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 00:07:04 2017

@author: Praca
"""

import boto3
import subprocess
import datetime

instance_id = 'i-0f1ed5a14c64d8373'
logs_path = '/home/ubuntu/logs/'

def stopEC2Instance(instance_id):
    ec2 = boto3.resource('ec2')
    ec2.instances.filter(InstanceIds=[instance_id]).stop()
    

date = datetime.datetime.today()
today = str(date.date()) + '_' + str(date.time())


process = subprocess.Popen('python /home/ubuntu/scripts/DD/EC2DataMartFBPicture.py > ' + logs_path + 'EC2DataMartFBPicture/' + today + '.txt', shell=True, stdout=subprocess.PIPE)
process.wait()
print(process.returncode)

process = subprocess.Popen('python /home/ubuntu/scripts/DD/EC2DataMartUser.py > ' + logs_path + 'EC2DataMartUser/' + today + '.txt', shell=True, stdout=subprocess.PIPE)
process.wait()
print(process.returncode)

print('Stopping EC2 instance')
stopEC2Instance(instance_id)