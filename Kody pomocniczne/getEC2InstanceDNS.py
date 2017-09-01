# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 12:39:32 2017

@author: Var
"""
import boto3

instance_id = 'i-0a521ec320bf0c7f6'



def getInstanceDNS(instance_id):
    client = boto3.client('ec2')
    response = client.describe_instances(InstanceIds = [instance_id])
    foo = response['Reservations'][0]['Instances'][0]['NetworkInterfaces'][0]['Association']['PublicDnsName']
    return foo

instance_dns = getInstanceDNS(instance_id)